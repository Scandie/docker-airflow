# -*- coding: utf-8 -*-
import logging
from socket import error

from airflow.models import Variable
from couchdb import Server, Session
from ZODB import DB
from ZEO import ClientStorage

from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.auctions import AuctionsClient
from openprocurement_client.resources.assets import AssetsClient

from concierge.core.constants import (
    CONFIG_AIRFLOW_VAR, ALLOWED_ASSET_TYPES_AIRFLOW_VAR,
    HANDLED_STATUSES, LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR,
    PATCH_ASSET_MESSAGE_ID, PATCH_LOT_MESSAGE_ID
)
from concierge.core.mapping import prepare_lot_mapping
from concierge.core.watchers import LastSeqNumber

CONTINUOUS_CHANGES_FEED_FLAG = True
EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)
STATUS_FILTER = """function(doc, req) {
  if(%s) {
        return true;
    }
    return false;
}"""

logger = logging.getLogger('openregistry.concierge.worker')

last_seq = LastSeqNumber()


class ConfigError(Exception):
    pass


# COUCH INITIALIZATION


def prepare_couchdb(couch_url, db_name, logger, errors_doc, couchdb_filter):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]

        broken_lots = db.get(errors_doc, None)
        if broken_lots is None:
            db[errors_doc] = {}

        prepare_couchdb_filter(db, 'lots', 'status', couchdb_filter, logger)

    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    return db


def prepare_couchdb_filter(db, doc, filter_name, filter, logger):
    design_doc = db['_design/{}'.format(doc)]
    if not design_doc.get('filters', ''):
        design_doc['filters'] = {}
    if filter_name not in design_doc['filters']:
        design_doc['filters'][filter_name] = filter
        logger.debug('Successfully created {0}/{1} filter.'.format(doc, filter_name))
    elif design_doc['filters'][filter_name] != filter:
        design_doc['filters'][filter_name] = filter
        logger.debug('Successfully updated {0}/{1} filter.'.format(doc, filter_name))
    else:
        logger.debug('Filter {0}/{1} already exists.'.format(doc, filter_name))
    db.save(design_doc)

# COUCH CONTINUOUS CHANGES FEED


def _prepare_doc_from_change(change):
    lot = change['doc']
    lot.update({'id': lot['_id'], 'rev': lot['_rev']})
    logger.info('Received Lot {} in status {}'.format(lot['id'], lot['status']))
    return lot


def _update_timeout(timeout, time_to_sleep):
    if timeout < time_to_sleep['max']:
        return timeout + time_to_sleep['step']
    else:
        return time_to_sleep['min']


def _set_heartbeat(time_to_sleep):
    if time_to_sleep.get('heartbeat'):
        return time_to_sleep['min']
    else:
        return None


def continuous_changes_feed(db, logger, time_to_sleep, filter_doc='lots/status', killer=None):
    last_seq = LastSeqNumber()
    timeout = time_to_sleep['min']
    heartbeat = _set_heartbeat(time_to_sleep)

    while CONTINUOUS_CHANGES_FEED_FLAG:
        last_seq.drop(logger)
        try:
            if killer.kill_now:
                break
            logger.info('Starting continuous changes feed')
            changes = db.changes(
                include_docs=True,
                since=last_seq.get(),
                filter=filter_doc,
                feed='continuous',
                # interval of dropping connection with sending last_seq
                timeout=timeout,
                # interval of sending of empty string to prevent connection drop, which ignores timeout option
                heartbeat=heartbeat
            )
        except error as e:
            logger.error('Failed to get lots from DB: [Errno {}] {}'.format(e.errno, e.strerror))
            break
        else:
            for change in changes:
                if change.get('doc'):  # change in actual documents
                    last_seq.set(change.get('seq', last_seq.get()))
                    yield _prepare_doc_from_change(change)
                    timeout = time_to_sleep['min'] - time_to_sleep['step']

                else:  # last_seq is sent by CouchDB due to timeout
                    last_seq.set(int(change.get('last_seq', last_seq.get())))
                    timeout = _update_timeout(timeout, time_to_sleep)

                if killer.kill_now:
                    break

            if killer.kill_now:
                break

            logger.info('Next continuous changes feed timeout: {}'.format(timeout / 1000))


# FILTERS CREATION


def create_certain_condition(place_to_check, items, condition):
    '''

    :param place_to_check: actually a variable or object in filter that should be checked
    :param items: values to check with value from place_to_check
    :param condition: type of condition to chain checks
    :return: condition in string

    >>> lot_aliases = ['loki', 'anotherLoki']
    >>> create_certain_condition('variable', lot_aliases, '&&')
    '(variable == "loki" && variable == "anotherLoki")'

    '''
    result = ''
    for item in items:
        if result:
            result = result + ' {} '.format(condition)
        result = result + place_to_check + ' == "{}"'.format(item)
    return '({})'.format(result) if result else ''


def create_filter_condition(lot_aliases, handled_statuses):
    '''
    :param lot_aliases: list of lot aliases
    :param handled_statuses: list of status that should be handled for certail lotType
    :return: condition that will be used in filter for couchdb

    >>> lot_aliases = ['loki', 'anotherLoki']
    >>> handled_statuses = ['pending', 'verification']
    >>> create_filter_condition(lot_aliases, handled_statuses)
    '(doc.lotType == "loki" || doc.lotType == "anotherLoki") && (doc.status == "pending" || doc.status == "verification")'
    >>> create_filter_condition(lot_aliases, [])
    '(doc.lotType == "loki" || doc.lotType == "anotherLoki")'
    >>> create_filter_condition([], handled_statuses)
    '(doc.status == "pending" || doc.status == "verification")'

    '''

    conditions = []

    conditions.append(create_certain_condition('doc.lotType', lot_aliases, '||'))
    conditions.append(create_certain_condition('doc.status', handled_statuses, '||'))

    filter_condition = ''

    for condition in conditions:
        if not condition:
            continue

        if filter_condition:
            filter_condition = filter_condition + ' && '

        filter_condition = filter_condition + condition

    return filter_condition


def get_condition(config, lot_type):
    return create_filter_condition(config.get('aliases', []), HANDLED_STATUSES[lot_type])

# CLIENTS INITIALIZATION


def init_clients(config, logger, couchdb_filter):
    clients_from_config = {
        'lots_client': {'section': 'lots', 'client_instance': LotsClient},
        'assets_client': {'section': 'assets', 'client_instance': AssetsClient},
        'auction_client': {'section': 'assets', 'client_instance': AuctionsClient}
    }
    result = ''
    exceptions = []

    # OP clients
    for key, item in clients_from_config.items():
        section = item['section']
        try:
            client = item['client_instance'](
                key=config[section]['api']['token'],
                host_url=config[section]['api']['url'],
                api_version=config[section]['api']['version']
            )
            clients_from_config[key] = client
            result = ('ok', None)
        except Exception as e:
            exceptions.append(e)
            result = ('failed', e)
        logger.check('{} - {}'.format(key, result[0]), result[1])
    store_clients_in_zodb('concierge', clients_from_config)

    # CouchDB client
    try:
        if config['db'].get('login', '') \
                and config['db'].get('password', ''):
            db_url = "http://{login}:{password}@{host}:{port}".format(
                **config['db']
            )
        else:
            db_url = "http://{host}:{port}".format(**config['db'])
        clients_from_config['db'] = prepare_couchdb(db_url, config['db']['name'], logger, config['errors_doc'], couchdb_filter)
        result = ('ok', None)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    logger.check('couchdb - {}'.format(result[0]), result[1])

    # Processed lots mapping check
    try:
        clients_from_config['lots_mapping'] = prepare_lot_mapping(
            config.get('lots_mapping', {}), check=True, logger=logger
        )
        result = ('ok', None)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    logger.check('lots_mapping - {}'.format(result[0]), result[1])

    if exceptions:
        raise exceptions[0]
    return clients_from_config


# ZEO OPERATIONS


def _get_zeo_server_db():
    config = Variable.get(CONFIG_AIRFLOW_VAR, deserialize_json=True)
    client_storage_config = config['client_storage']
    addr = client_storage_config['host'], client_storage_config['port']
    storage = ClientStorage.ClientStorage(addr)
    return DB(storage)


def store_clients_in_zodb(scope, clients):
    db = _get_zeo_server_db()

    with db.transaction() as connection:
        if not connection.root().get(scope):
            connection.root()[scope] = {}

        connection.root()[scope].update({
            key: client for key, client in clients.items()
        })


def get_client_from_zodb(scope, client):
    db = _get_zeo_server_db()

    with db.transaction() as connection:
        client = connection.root()[scope][client]
    return client

# API OPERATIONS


def check_assets(lot, status='pending', assets_getter=None, related_lot_getter = None):
    """
    Makes GET request to openregistry for every asset id in assets list
    from lot object, passed as parameter, with client specified in
    configuration file.

    Args:
        lot: dictionary which contains some fields of lot
             document from db: id, rev, status, assets, lotID.
        status (str): status, in which assets are considered
                      as available. Defaults to 'pending'.
        assets_getter: function for getting lot assets by certain criteria.

    Returns:
        bool: True if request was successful and conditions were
              satisfied, False otherwise.

    Raises:
        RequestFailed: if RequestFailed was raised during request.
    """
    assets_client = get_client_from_zodb('concierge', 'assets_client')
    allowed_asset_types = Variable.get(ALLOWED_ASSET_TYPES_AIRFLOW_VAR, deserialize_json=True)
    lot_type_configurator = Variable.get(LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR, deserialize_json=True)
    lot_type = lot_type_configurator[lot['lotType']]

    if assets_getter:
        assets = assets_getter(lot)
    else:
        assets = lot['assets']

    for asset_id in assets:
        try:
            asset = assets_client.get_asset(asset_id).data
            logger.info('Successfully got asset {}'.format(asset_id))
        except ResourceNotFound as e:
            logger.error('Failed to get asset {0}: {1}'.format(asset_id,
                                                               e.message))
            return False
        except RequestFailed as e:
            logger.error('Failed to get asset {0}. Status code: {1}'.format(asset_id, e.status_code))
            raise e
        if asset.assetType not in allowed_asset_types[lot_type]:
            return False
        if related_lot_getter:
            related_lot = related_lot_getter(asset)
            related_lot_check = related_lot and related_lot.relatedProcessID != lot['id']
        else:
            related_lot_check = 'relatedLot' in asset and asset.relatedLot != lot['id']
        if related_lot_check or asset.status != status:
            return False
    return True


def check_lot(lot, lot_type):
    """
    Makes GET request to openregistry by client, specified in configuration
    file, with lot id from lot object, passed as parameter.

    Args:
        lot: dictionary which contains some fields of lot
             document from db: id, rev, status, assets, lotID.
    Returns:
        bool: True if request was successful and conditions were
              satisfied, False otherwise.
    """
    lots_client = get_client_from_zodb('concierge', 'lots_client')

    try:
        actual_status = lots_client.get_lot(lot['id']).data.status
        logger.info('Successfully got Lot {0}'.format(lot['id']))
    except ResourceNotFound as e:
        logger.error('Failed to get Lot {0}: {1}'.format(lot['id'], e.message))
        return False
    except RequestFailed as e:
        logger.error('Failed to get Lot {0}. Status code: {1}'.format(lot['id'], e.status_code))
        raise e
    if lot['status'] != actual_status:
        logger.warning(
            "Lot {0} status ('{1}') already changed to ('{2}')".format(lot['id'], lot['status'], actual_status))
        return False
    if lot['status'] not in HANDLED_STATUSES[lot_type]:
        logger.warning("Lot {0} can not be processed in current status ('{1}')".format(lot['id'], lot['status']))
        return False
    return True


def get_asset_id_from_related_process(lot):
    assets = [rP['relatedProcessID'] for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
    return assets


def get_asset_related_lot(asset):
    for rP in asset.get('relatedProcesses', []):
        if rP.type == 'lot':
            return rP


def patch_assets(assets, status, extras={}):
    """
    Makes PATCH request to openregistry for every asset id in assets list
    from lot object, passed as parameter, with client specified in
    configuration file. PATCH request will replace values of fields 'status' and
    'relatedLot' of asset with values passed as parameters 'status' and
    'related_lot' respectively.

    Args:
        assets: list of assets to patch.
        status (str): status, assets will be patching to.
        extras: extra data to patch along with status.

    Returns:
        tuple: (
            bool: True if request was successful and conditions were
                  satisfied, False otherwise.
            list: list with assets, which were successfully patched.
        )
    """
    patched_assets = []
    is_all_patched = True

    patch_data = {"status": status}
    if extras:
        patch_data.update(extras)

    for asset_id in assets:
        try:
            patch_single_asset(asset_id, patch_data)
        except EXCEPTIONS as e:
            is_all_patched = False
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error("Failed to patch asset {} to {} ({})".format(asset_id, status, message))
        else:
            patched_assets.append(asset_id)
    return is_all_patched, patched_assets


def patch_single_asset(asset_id, patch_data):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    assets_client.patch_asset(
        asset_id,
        {"data": patch_data}
    )
    logger.info("Successfully patched asset {} to {}".format(asset_id, patch_data['status']),
                extra={'MESSAGE_ID': PATCH_ASSET_MESSAGE_ID})


def patch_lot(lot, status, extras=None):
    """
    Makes PATCH request to openregistry for lot id from lot object,
    passed as parameter, with client specified in configuration file.

    Args:
        lot: dictionary which contains some fields of lot
             document from db: id, rev, status, assets, lotID.
        status (str): status, lot will be patching to.

    Returns:
        bool: True if request was successful and conditions were
              satisfied, False otherwise.
    """
    if not extras:
        extras = {}
    lots_client = get_client_from_zodb('concierge', 'lots_client')
    try:
        patch_data = {"status": status}
        if extras:
            patch_data.update(extras)
        lots_client.patch_lot(lot['id'], {"data": patch_data})
    except EXCEPTIONS as e:
        message = e.message
        if e.status_code >= 500:
            message = 'Server error: {}'.format(e.status_code)
        logger.error("Failed to patch Lot {} to {} ({})".format(lot['id'], status, message))
        return False
    else:
        logger.info("Successfully patched Lot {} to {}".format(lot['id'], status),
                    extra={'MESSAGE_ID': PATCH_LOT_MESSAGE_ID})
        return True


def retry_on_error(exception):
    """
    :param exception: python Exception object
    :return: True if passed exception is in default EXCEPTIONS list, otherwise False
    :rtype: bool
    """
    if isinstance(exception, EXCEPTIONS) and (exception.status_code >= 500 or exception.status_code in [409, 412, 429]):
        return True
    return False
