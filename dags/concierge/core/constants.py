# -*- coding: utf-8 -*-
import os
from datetime import timedelta

from pytz import timezone

ALLOWED_ASSET_TYPES_AIRFLOW_VAR = 'allowed_asset_types'
LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR = 'concierge_handled_lot_types'
PLANNED_PMT_AIRFLOW_VAR = 'planned_pmts'
CONFIG_AIRFLOW_VAR = 'concierge_config'
RETRIES = 5
RETRY_DELAY = timedelta(seconds=2)

HANDLED_STATUSES = {
    'loki': ('verification', 'pending.dissolution', 'pending.sold', 'pending.deleted', 'active.salable'),
    'basic': ('verification', 'recomposed', 'pending.dissolution', 'pending.sold', 'pending.deleted'),
}

DEFAULTS = {
    "version": 1,
    "db": {
        "host": "127.0.0.1",
        "name": "lots_db",
        "port": "5984",
        "login": "",
        "password": "",
        "filter": "lots/status"
    },
    "errors_doc": "broken_lots",
    "time_to_sleep": {
        'min': 60000,
        'max': 180000,
        'step': 20000,
        'heartbeat': False
    },
    "lots": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
        },
        "basic": {
            'aliases': ["basic"],
            'assets': {
                "basic": ["basic"],
                "compound": ["compound"],
                "claimRights": ["claimRights"]
            }
        },
        "loki": {
            "planned_pmt": [],
            'aliases': ["loki"],
            'assets': {
                "bounce": ["bounce", "domain"]
            }
        }
    },
    "assets": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
        }
    },
    "auctions": {
        "api": {
            "url": "http://0.0.0.0:6543",
            "token": "concierge",
            "version": 0.1
        }
    },
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        "openregistry.concierge.worker": {
            "handlers": ["console"],
            "propagate": "no",
            "level": "DEBUG"
        },
        "": {
            "handlers": ["console"],
            "level": "DEBUG"
        }
    }
}


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')

AUCTION_CREATE_MESSAGE_ID = 'create_auction'
FAILED_AUCTION_CREATE_MESSAGE_ID = 'failed_to_create_auction'
PATCH_ASSET_MESSAGE_ID = 'patch_asset'
PATCH_LOT_MESSAGE_ID = 'patch_lot'
SKIP_LOT_MESSAGE_ID = 'skip_lot'
PROCESS_BASIC_LOT_MESSAGE_ID = 'process_basic_lot'
PROCESS_LOKI_LOT_MESSAGE_ID = 'process_loki_lot'
GETTING_LOTS_MESSAGE_ID = 'getting_lots'
