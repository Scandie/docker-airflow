# -*- coding: utf-8 -*-
import logging

from airflow.models import Variable

from concierge.core.constants import CONFIG_AIRFLOW_VAR
from concierge.core.utils import patch_assets, patch_lot, check_assets
from concierge.core.mapping import prepare_lot_mapping

from concierge.loki.utils import patch_lot_asset_related_processes

logger = logging.getLogger('openregistry.concierge.worker')

# REUSABLE CALLABLE FOR OPERATORS


def get_item_from_direct_upstream_tasks(key='return_value', **context):
    """
    Extract object by provided key, that was pushed to xcom by upstream task

    :param key: key for getting object by from xcom
    :param context: airflow task context

    :return: object from xcom
    """
    results = context['task_instance'].xcom_pull(
        dag_id=context['task'].dag_id,
        task_ids=context['task'].upstream_task_ids,
        key=key
    )
    results = [result for result in results if result]
    return results[0]


def patch_assets_to(status,
                    lot_doc=None,
                    lot_id=False,
                    on_fail=None,
                    on_success=True,
                    assets_subset=None,
                    assets_getter=None,
                    message=None,
                    **context):
    """
    Reusable callable for airflow tasks, responsible for assets patching

    :param status: status, which assets will be switched to
    :type status: str
    :param lot_doc: lot object, which is being processing
                    if not provided, lot object will be extracted from xcom
    :type lot_doc: dict
    :param lot_id: parameter, responsible for creating extra data
                   ignored when False, used as value for 'relatedLot' field of
                   extra data dictionary, else serves as flag for setting lot_doc['id']
                   as value for 'relatedLot' field of extra data dictionary
    :param on_fail: task_id of task that should be ran in case patching failed
    :type on_fail: str
    :param on_success: task_id of task that should be ran in case patching succeed
    :type on_success: str
    :param assets_subset: flag for getting collection of ids of assets, that should
                          be patched, from xcom if True, otherwise all assets that
                          associated with lot will be patched
    :type assets_subset: bool
    :param assets_getter: callable, responsible for specific assets extracting from lot
                          by passing lot object to this callable
                          if not provided, assets will be taken from 'assets' key of
                          lot object
    :param message: template for logging message before actual patching operations
    :type message: str
    :param context: airflow task context

    :return: task_id of task, that should be ran next
    :rtype: str

    :raises: ValueError in case not all assets were patched and on_fail parameter is not provided

    """
    if not lot_doc:
        lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    if assets_subset:
        # Getting list of patched assets, that was xcom-pushed in upstream task
        assets = get_item_from_direct_upstream_tasks('patched_assets', **context)
        container = {'assets': assets}
    else:
        container = lot_doc

    # Extracting assets associated with lot
    assets = assets_getter(container) if assets_getter else container.get('assets')
    if not assets:
        return on_success

    if message:
        logger.info(message.format(assets, status))

    # Creating dictionary with extra data to patch
    if lot_id is not False:
        extras = {'relatedLot': None if lot_id is None else lot_doc['id']}
    else:
        extras = {}

    result, patched_assets = patch_assets(assets, status, extras)
    # Pushing ids of patched assets to xcom
    context['task_instance'].xcom_push('patched_assets', patched_assets)
    if result is False:

        if not on_fail or not patched_assets:
            raise ValueError(
                'Can`t patch assets of lot {} to {}'.format(
                    lot_doc['id'],
                    status
                )
            )
        # TODO 'log broken lot' ??
        return on_fail
    else:
        return on_success


def patch_lot_to(status, lot_doc=None, **context):
    """
    Reusable callable for airflow tasks, responsible for lot patching

    :param status: status, which lot will be switched to
    :type status: str
    :param lot_doc: lot object, status of which will be switched
                    if not provided, lot object will be extracted from xcom
    :type lot_doc: dict
    :param context: airflow task context
    :return: None

    :raises: ValueError in case lot was not patched
    """
    if not lot_doc:
        lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    result = patch_lot(
        lot_doc,
        status
    )

    if result is False:
        # TODO 'log broken lot'
        raise ValueError('Lot {} was not moved to {}'.format(lot_doc['id'], status))


def check_assets_available(on_fail=None,
                           on_success=True,
                           status='pending',
                           assets_getter=None,
                           related_lot_getter=None,
                           **context):
    """
    Reusable callable for airflow tasks, responsible for checking assets availability

    :param on_fail: task_id of task that should be ran in case assets are not available
    :type on_fail: str
    :param on_success: task_id of task that should be ran in case assets are available
    :type on_success: str
    :param status: status, in which assets are considered as available. Defaults to 'pending'.
    :type status: str
    :param assets_getter: callable, responsible for specific assets extracting from lot
                          by passing lot object to this callable
                          if not provided, assets will be taken from 'assets' key of
                          lot object
    :param related_lot_getter: callable, responsible for specific extracting of relatedLot
                               value from asset by passing asset object to this callable
                               if not provided, relatedLot will be taken from 'relatedLot' key of
                               asset object
    :param context: airflow task context

    :return: task_id of task, that should be ran next
    :rtype: str

    :raises: ValueError in case assets are not available and on_fail parameter is not provided
    """
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')

    result = check_assets(lot_doc, status, assets_getter, related_lot_getter)

    if result:
        return on_success
    else:
        if not on_fail:
            raise ValueError(
                'Assets of lot {} are not available'.format(lot_doc['id'])
            )
        return on_fail


def patch_related_processes(on_fail=None,
                            on_success=True,
                            patched_rPs=False,
                            cleanup=False,
                            **context):
    """

    :param on_fail: task_id of task that should be ran in case lot assets
                    related processes were not patched successfully
    :type on_fail: str
    :param on_success: task_id of task that should be ran in case lot assets
                       related processes were patched successfully
    :type on_success: str
    :param patched_rPs: flag for getting collection of lot assets related
                        processes, that should be patched, from xcom if True,
                        otherwise all lot assets related processes  will be patched
    :type patched_rPs: bool
    :param cleanup: flag, for removing association with asset from lot if True,
                    otherwise creating it
    :type cleanup: bool
    :param context: airflow task context

    :return: task_id of task, that should be ran next
    :rtype: str

    :raises: ValueError in case lot assets related processes wre not patched
             and on_fail parameter is not provided
    """

    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    if patched_rPs:
        # Getting list of patched rPs, that was xcom-pushed in upstream task
        relatedProcesses = get_item_from_direct_upstream_tasks('patched_rPs', **context)
        container = {'id': lot_doc['id'], 'relatedProcesses': relatedProcesses}
    else:
        container = lot_doc

    result, patched_rPs = patch_lot_asset_related_processes(container, cleanup)
    context['task_instance'].xcom_push('patched_rPs', patched_rPs)

    if result is False:
        if not on_fail:
            raise ValueError('Can`t clean up lot.related_processes')
            # TODO 'log broken lot' ??
        return on_fail
    else:
        return on_success


# CALLBACKS

def release_lot(context):
    """
    Removing lot id from lots mapping, which making this lot available to process

    :param context: airflow task context
    :return: None
    """
    # extracting lot from xcom
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    config = Variable.get(CONFIG_AIRFLOW_VAR, deserialize_json=True)
    lots_mapping = prepare_lot_mapping(
            config.get('lots_mapping', {}), check=True, logger=logger
        )

    if lots_mapping.has(lot_doc['id']):
        lots_mapping.delete(lot_doc['id'])
