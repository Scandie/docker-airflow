# -*- coding: utf-8 -*-
import argparse
import os
from copy import deepcopy

import yaml
from couchdb import Server, Session

from dags.concierge.core.constants import DEFAULTS
from snapshots import bounce_asset, basic_asset, loki_lot, basic_lot


def create_lot_asset_pair(db, lot_type):
    asset1 = bounce_asset if lot_type == 'loki' else basic_asset
    asset = deepcopy(asset1)
    asset_id, _ = db.save(asset)
    doc = db[asset_id]
    doc['id'] = asset_id
    db[asset_id] = doc

    lot1 = loki_lot if lot_type == 'loki' else basic_lot
    lot = deepcopy(lot1)
    if lot_type == 'loki':
        lot['relatedProcesses'][0]['relatedProcessID'] = asset_id
    else:
        lot['assets'] = [asset_id]
    lot_id, _ = db.save(lot)
    doc = db[lot_id]
    doc['id'] = lot_id
    doc['status'] = u'verification'
    db[lot_id] = doc


def clear_db(server, db_name):
    del server[db_name]


def main():
    parser = argparse.ArgumentParser(description='---- Lot Assets creation ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('-n', '--number', dest='number', type=str,
                        help='Number of lots to create', default='0')
    parser.add_argument('-c', dest='clear', action='store_const',
                        const=True, default=False,
                        help='Wipe the db')
    parser.add_argument('-t', '--type', dest='type', type=str,
                        help='type of lot to create', default='loki')
    params = parser.parse_args()
    config = {}
    if os.path.isfile(params.config):
        with open(params.config) as config_object:
            config = yaml.load(config_object.read())
        # logging.config.dictConfig(config)
    DEFAULTS.update(config)

    if DEFAULTS['db'].get('login', '') \
            and DEFAULTS['db'].get('password', ''):
        db_url = "http://{login}:{password}@{host}:{port}".format(
            **DEFAULTS['db']
        )
    else:
        db_url = "http://{host}:{port}".format(**DEFAULTS['db'])

    db_name = DEFAULTS['db']['name']
    server = Server(db_url, session=Session(retry_delays=range(10)))

    if db_name not in server:
        db = server.create(db_name)
    else:
        db = server[db_name]
    wipe = params.clear
    if wipe:
        clear_db(server, db_name)
        exit()
    lot_asset_count = int(params.number)
    for _ in range(lot_asset_count):
        create_lot_asset_pair(db, params.type)


if __name__ == '__main__':
    main()
