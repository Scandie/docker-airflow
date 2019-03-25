# -*- coding: utf-8 -*-
import logging
from re import compile

from retrying import retry
from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from concierge.core.constants import AUCTION_CREATE_MESSAGE_ID
from concierge.core.utils import get_client_from_zodb, retry_on_error

logger = logging.getLogger('openregistry.concierge.worker')

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)

ACCELERATOR_RE = compile(r'.accelerator=(?P<accelerator>\d+)')
HANDLED_AUCTION_STATUSES = ('scheduled', 'unsuccessful')
KEYS_FOR_AUCTION_CREATE = {
    'title': 'title',
    'mode': 'mode',
    'merchandisingObject': 'id',
    'description': 'description',
    'tenderAttempts': 'auctions/{}/tenderAttempts',
    'procuringEntity': 'lotCustodian',
    'items': 'items',
    'value': 'auctions/{}/value',
    'minimalStep': 'auctions/{}/minimalStep',
    'guarantee': 'auctions/{}/guarantee',
    'registrationFee': 'auctions/{}/registrationFee',
    'procurementMethodType': 'auctions/{}/procurementMethodType',
    'documents': 'auctions/{}/documents',
    'bankAccount': 'auctions/{}/bankAccount',
    'auctionParameters': 'auctions/{}/auctionParameters',
    'procurementMethodDetails': 'auctions/{}/procurementMethodDetails',
    'submissionMethodDetails': 'auctions/{}/submissionMethodDetails',
    'submissionMethodDetails_en': 'auctions/{}/submissionMethodDetails_en',
    'submissionMethodDetails_ru': 'auctions/{}/submissionMethodDetails_ru',
    'contractTerms/type': 'contracts/0/type'
}

# API OPERATIONS


def patch_lot_related_process(data, lot_id, related_process_id):
    lots_client = get_client_from_zodb('concierge', 'lots_client')
    auction = lots_client.patch_resource_item_subitem(
        resource_item_id=lot_id,
        patch_data={'data': data},
        subitem_name='related_processes',
        subitem_id=related_process_id
    )
    logger.info("Successfully patched Lot.relatedProcess {} from Lot {})".format(related_process_id, lot_id))
    return auction


@retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
def patch_lot_auction(data, lot_id, auction_id):
    lots_client = get_client_from_zodb('concierge', 'lots_client')
    auction = lots_client.patch_resource_item_subitem(
        resource_item_id=lot_id,
        patch_data={'data': data},
        subitem_name='auctions',
        subitem_id=auction_id
    )
    logger.info("Successfully patched Lot.auction {} from Lot {})".format(auction_id, lot_id))
    return auction


def extract_lot_transfer_token(lot_id):
    lots_client = get_client_from_zodb('concierge', 'lots_client')

    credentials = lots_client.extract_credentials(resource_item_id=lot_id)
    logger.info("Successfully extracted transfer_token from Lot {})".format(lot_id))
    return credentials['data']['transfer_token']


def patch_lot_asset_related_processes(lot, cleanup=False):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    related_processes = [rP for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
    patched_rPs = []
    is_all_patched = True
    for rP in related_processes:
        asset = assets_client.get_asset(rP['relatedProcessID']).data
        if not cleanup:
            data = {'identifier': asset['assetID']}
        else:
            data = {'identifier': ''}
        try:
            patch_lot_related_process(data, lot['id'], rP['id'])
        except EXCEPTIONS as e:
            is_all_patched = False
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error("Failed to patch relatedProcess {} in Lot {} ({})".format(rP['id'], lot['id'], message))
        else:
            patched_rPs.append(rP)
    return is_all_patched, patched_rPs


@retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
def post_auction(data, lot_id):
    auction_client = get_client_from_zodb('concierge', 'auction_client')

    auction = auction_client.create_auction(data)
    logger.info(
        "Successfully created auction {} from Lot {})".format(auction['data']['id'], lot_id),
        extra={'MESSAGE_ID': AUCTION_CREATE_MESSAGE_ID}
    )
    return auction


def create_asset_related_process(asset_id, data):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    related_process = assets_client.create_resource_item_subitem(
        resource_item_id=asset_id,
        subitem_obj={'data': data},
        subitem_name='related_processes',
    )
    logger.info("Successfully post Asset.relatedProcess from Asset {})".format(asset_id))
    return related_process


def remove_asset_lot_related_process(asset_id, related_process_id):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    related_process = assets_client.delete_resource_item_subitem(
        resource_item_id=asset_id,
        subitem_name='related_processes',
        subitem_id=related_process_id
    )
    logger.info("Successfully remove Asset.relatedProcess from Asset {})".format(asset_id))
    return related_process


def get_assets_related_processes(lot):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    related_processes = []
    lot_assets_related_processes = [
        rP for rP in lot.get('relatedProcesses', [])
        if rP['type'] == 'asset'
    ]

    for lrp in lot_assets_related_processes:

        asset = assets_client.get_asset(lrp['relatedProcessID']).data

        asset_related_processes = [
            rP for rP in asset.get('relatedProcesses', [])
            if rP['type'] == 'lot' and rP['relatedProcessID'] == lot['id']
        ]

        for arp in asset_related_processes:
            arp['asset_parent'] = asset['id']
            related_processes.append(arp)

    return related_processes


# HELPER FUNCTIONS

def _clean_asset_related_processes(assets_rPs):
    is_all_patched = True
    for rP in assets_rPs:
        try:
            remove_asset_lot_related_process(rP['asset_parent'], rP['id'])
        except EXCEPTIONS as e:
            is_all_patched = False
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error("Failed to clean relatedProcess {} in Asset {} ({})".format(rP['id'], rP['asset_parent'], message))
    return is_all_patched


def make_lot_related_process(lot):
    return {
        'type': 'lot',
        'relatedProcessID': lot['id'],
        'identifier': lot['lotID']
    }


def check_previous_auction(lot, status='unsuccessful'):
    for index, auction in enumerate(lot['auctions']):
        if auction['status'] == 'scheduled':
            if index == 0:
                return True
            previous = lot['auctions'][index - 1]
            return previous['status'] == status
    else:
        return False


def _add_related_process_to_assets(lot):
    related_process_type_asset = [rP for rP in lot['relatedProcesses'] if rP['type'] == 'asset']
    patched_rPs = []
    is_all_patched = True
    lot_related_process_data = make_lot_related_process(lot)

    for rP in related_process_type_asset:
        try:
            created_rP = create_asset_related_process(rP['relatedProcessID'], lot_related_process_data)
            created_rP['asset_parent'] = rP['relatedProcessID']
        except EXCEPTIONS as e:
            is_all_patched = False
            message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
            logger.error(
                "Failed to add relatedProcess of lot {} in Asset {} ({})".format(
                    lot['id'],
                    rP['relatedProcessID'],
                    message
                )
            )
        else:
            patched_rPs.append(created_rP)
    return is_all_patched, patched_rPs
