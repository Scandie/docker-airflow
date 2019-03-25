# -*- coding: utf-8 -*-
from mapping_types import (
    MAPPING_TYPES,
    MappingConfigurationException
)


def prepare_lot_mapping(config, logger, check=False):
    """
    Initialization of lots_mapping, which are used for tracking lots,
    which already were processed by concierge.
    :param config: configuration for lots_mapping
    :type config: dict
    :param check: run doctest if set to True
    :type check: bool
    :return: lots_mapping instance
    :rtype: LotsMapping
    """
    mapping_class = MAPPING_TYPES.get(config.get('type', 'void'), None)

    if mapping_class is None:
        raise MappingConfigurationException('Only `void`, `redis` and `lazy` types are available')

    db = mapping_class(config, logger)

    return db
