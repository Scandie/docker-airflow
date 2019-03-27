# -*- coding: utf-8 -*-
import os
import re
from copy import deepcopy

import yaml
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = '{}/../configs'.format(CURRENT_DIR)  # TODO add config getter
WORKERS_CONFIGS_PATTERN = re.compile(r'^.*\.yaml$')
LOGGER_CONFIG_KEYS = ['handlers', 'formatters', 'loggers']


def prepare_logging_config():

    logging_config = deepcopy(DEFAULT_LOGGING_CONFIG)

    for filename in os.listdir(CONFIGS_DIR):
        if not WORKERS_CONFIGS_PATTERN.match(filename):
            continue
        with open('{}/{}'.format(CONFIGS_DIR, filename)) as config_object:
            config = yaml.load(config_object.read())
        for key in LOGGER_CONFIG_KEYS:
            logging_config[key].update(config[key])
    return logging_config


LOGGING_CONFIG = prepare_logging_config()
