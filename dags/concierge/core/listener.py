# -*- coding: utf-8 -*-
import signal
from logging import addLevelName, Logger, getLogger

from airflow.models import Variable

from concierge.core.constants import (
    ALLOWED_ASSET_TYPES_AIRFLOW_VAR,
    LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR,
    PLANNED_PMT_AIRFLOW_VAR, GETTING_LOTS_MESSAGE_ID
)
from concierge.core.utils import (
    get_condition, STATUS_FILTER, init_clients,
    continuous_changes_feed
)

logger = getLogger('openregistry.concierge.worker')

addLevelName(25, 'CHECK')


def check(self, msg, exc=None, *args, **kwargs):
    self.log(25, msg)
    if exc:
        self.error(exc, exc_info=True)


Logger.check = check


class GracefulKiller(object):
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


class Concierge(object):
    def __init__(self, config):
        """
        Args:
            config: dictionary with configuration data
        """
        self.config = config
        self.killer = GracefulKiller()

        filter_condition = '({}) || ({})'.format(
            get_condition(self.config['lots']['types']['loki'], 'loki'),
            get_condition(self.config['lots']['types']['basic'], 'basic')
        )
        couchdb_filter = STATUS_FILTER % filter_condition

        self._register_handled_lot_types()

        self.created_clients = init_clients(config, logger, couchdb_filter)
        self.sleep = self.config['time_to_sleep']
        self.errors_doc = self.created_clients['db'].get(self.config['errors_doc'], {})
        self.patch_log_doc = self.created_clients['db'].get('patch_requests')

    def get_lot(self):
        """
        Receiving lots from db, which are filtered by CouchDB filter
        function specified in the configuration file.

        Returns:
            generator: Generator object with the received lots.
        """
        logger.info('Getting Lots', extra={'MESSAGE_ID': GETTING_LOTS_MESSAGE_ID})
        return continuous_changes_feed(
            self.created_clients['db'], logger, self.sleep,
            filter_doc=self.config['db']['filter'], killer=self.killer
        )

    def _register_handled_lot_types(self):
        """
        Setting supported lot types, auction procurementMethodTypes, assets
        and their aliases to Airflow Variables
        """
        self.lot_type_configurator = {}
        self.allowed_asset_types = {}
        self.planned_pmts = {}
        lots_types_config = self.config['lots']['types']

        for lot_type in lots_types_config:
            self.allowed_asset_types[lot_type] = []

            # Registering lots aliases
            aliases = lots_types_config[lot_type].get('aliases', [])
            for alias in aliases:
                self.lot_type_configurator[alias] = lot_type

            # Registering assets aliases
            for _, asset_aliases in lots_types_config[lot_type].get('assets', {}).items():
                self.allowed_asset_types[lot_type] += asset_aliases

            # Registering planned procurement method types
            planned_pmts = lots_types_config[lot_type].get('planned_pmt', None)
            if planned_pmts:
                self.planned_pmts[lot_type] = planned_pmts

        Variable.set(ALLOWED_ASSET_TYPES_AIRFLOW_VAR, self.allowed_asset_types, serialize_json=True)
        Variable.set(LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR, self.lot_type_configurator, serialize_json=True)
        Variable.set(PLANNED_PMT_AIRFLOW_VAR, self.planned_pmts, serialize_json=True)
