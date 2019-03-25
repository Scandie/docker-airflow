# -*- coding: utf-8 -*-
from functools import partial
from validx import Dict, Str, List, Int, Any, Bool

Str = partial(Str, encoding='utf-8')

# Db config
db_conf = Dict(
    {
        'host': Str(),  # url where db is located
        'name': Str(),  # name of db to work with
        'port': Str(),  # port on which db is located
        'login': Str(),  # login credential to access db if needed
        'password': Str(),  # password credential to access db if needed
        'filter': Str(),  # filter that should be used to filter lot by certain conditions
    },
    defaults={
        'filter': 'lots/status',
        'name': 'lots_db',
    }
)

# lots mapping config
lots_mapping_conf = Dict(
    {
        'type': Str(),  # Type of mapping that should be used to temporarily save processed lots
        'host': Str(),  # Host address for redis mapping type
        'port': Str()   # Port for redis mapping type
    },
    defaults={
        'type': 'void'
    }
)

# time to sleep
time_to_sleep = Dict(
    {
        'min': Int(),   # Min interval in milliseconds before dropping changes feed connection
        'max': Int(),   # Max interval in milliseconds before dropping changes feed connection
        'step': Int(),  # Step of increasing timeout interval in milliseconds
        'heartbeat': Bool()  # Marker for using heartbeat parameter (instead of timeout)
    },
    defaults={
        'min': 60000,
        'max': 180000,
        'step': 20000,
        'heartbeat': False
    }
)

client_storage_conf = Dict(
    {
        'host': Str(),  # Host address for ZEO db
        'port': Int()   # Port for ZEO db
    },
    defaults={
        'host': 'localhost',
        'port': 9100
    }
)


# Validator that can be used in many components
api_conf = Dict(
    {
        'url': Str(),  # url to api
        'token': Str(),  # token that give you permission on some action
        'version': Str(),  # version of api
    }
)

# lots configuration consists of api, loki, basic(loki and basic are different component for processing different lot)
loki = Dict(
    {
        'planned_pmt': List(Str()),  # Define procurement method type that can be planned from this lot type
        'aliases': List(Str()),  # Define aliases for loki lot type
        'assets': Dict({  # Define assets that can be connected to this type of lot
            'bounce': List(Str())  # Define aliases for this type of asset
        })
    }
)

basic = Dict(
    {
        'aliases': List(Str()),  # Define aliases for basic lot type
        'assets': Dict({  # Define assets that can be connected to this type of lot
            'basic': List(Str()),  # Define aliases for this type of asset
            'compound': List(Str()),  # same ^
            'claimRights': List(Str())  # same ^
        })
    }
)
lot_types = Dict(
    {
        'loki': loki,  # configuration for processing of loki lots
        'basic': basic  # configuration for processing of basic lots
    }
)
lot_conf = Dict(
    {
        'api': api_conf,  # configuration of api where lots is stored
        'types': lot_types,

    }
)
# assets

assets_conf = Dict(
    {
        'api': api_conf
    }
)

# auctions
auctions_conf = Dict(
    {
        'api': api_conf
    }
)

# concierge config

concierge = Dict(
    {
        'db': db_conf,  # configuration for db
        'lots': lot_conf,  # configuration for lots processing and lots api
        'assets': assets_conf,  # configuration for assets api
        'auctions': auctions_conf,  # configuration for auctions api
        'lots_mapping': lots_mapping_conf,  # configuration for mapping which store temporarily lot ids
        'client_storage': client_storage_conf,  # configuration for shared storage for api clients
        'time_to_sleep': time_to_sleep,  # configuration for timeouts during continuous changes feed polling
        'errors_doc': Str(),  # document where stored docs during processing which errors occured
        'version': Int(),  # Version of logging dictConfig
        # Logging config
        'loggers': Any(),  # loggers configuration
        'handlers': Any(),  # configuration for loggers handlers
        'formatters': Any(),  # configuration for loggers formatters
    },
    defaults={
        'errors_doc': 'broken_lots',
        'version': 1,
        'time_to_sleep': time_to_sleep.defaults
    }
)
