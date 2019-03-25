# -*- coding: utf-8 -*-
import logging
from copy import deepcopy
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from dpath import util
from isodate import parse_duration
from iso8601 import parse_date

from concierge.core.calculate_date import calculate_business_date
from concierge.core.constants import (
    PLANNED_PMT_AIRFLOW_VAR, RETRIES, RETRY_DELAY,
    FAILED_AUCTION_CREATE_MESSAGE_ID, TZ
)

from concierge.core.dags_utils import (
    patch_lot_to, check_assets_available,
    get_item_from_direct_upstream_tasks, release_lot
)
from concierge.core.utils import (
    get_asset_id_from_related_process, get_asset_related_lot
)

from concierge.loki.related_processes_subflow import remove_assets_from_lot
from concierge.loki.utils import (
    EXCEPTIONS,
    KEYS_FOR_AUCTION_CREATE,
    HANDLED_AUCTION_STATUSES,
    ACCELERATOR_RE,
    patch_lot_auction,
    post_auction,
    extract_lot_transfer_token
)

logger = logging.getLogger('openregistry.concierge.worker')

# Utils


def dict_from_object(keys, obj, auction_index):
    to_patch = {}
    for to_key, from_key in keys.items():
        try:
            value = util.get(obj, from_key.format(auction_index))
        except KeyError:
            continue
        util.new(
            to_patch,
            to_key,
            value
        )
    return to_patch


def get_next_auction(lot):
    auctions = filter(lambda a: a['status'] == 'scheduled', lot['auctions'])
    return auctions[0] if auctions else {}


def check_previous_auction(lot, status='unsuccessful'):
    for index, auction in enumerate(lot['auctions']):
        if auction['status'] == 'scheduled':
            if index == 0:
                return True
            previous = lot['auctions'][index - 1]
            return previous['status'] == status
    else:
        return False


# Workflow

def check_auctions_availability(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')

    is_all_auction_valid = all([a['status'] in HANDLED_AUCTION_STATUSES for a in lot_doc['auctions']])
    previous_auction_has_valid_status = check_previous_auction(lot_doc)

    if not (is_all_auction_valid and previous_auction_has_valid_status):
        raise ValueError('No auctions available')


def check_procurement_method_type(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')
    allowed_pmts = Variable.get(PLANNED_PMT_AIRFLOW_VAR, deserialize_json=True)
    auction_from_lot = get_next_auction(lot_doc)
    if not auction_from_lot:
        raise ValueError('There is no auction in status `scheduled`')
    if auction_from_lot['procurementMethodType'] not in allowed_pmts['loki']:
        logger.warning(
            "Such procurementMethodType is not allowed to create {}. "
            "Allowed procurementMethodType {}".format(
                auction_from_lot['procurementMethodType'], allowed_pmts['loki']
            )
        )
        raise ValueError()


def prepare_basic_data(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')

    auction_from_lot = get_next_auction(lot_doc)

    auction = dict_from_object(KEYS_FOR_AUCTION_CREATE, lot_doc, auction_from_lot['tenderAttempts'] - 1)
    lot_documents = deepcopy(lot_doc.get('documents', []))
    for d in lot_documents:
        if d['documentOf'] == 'lot':
            d['relatedItem'] = lot_doc['id']
    auction.setdefault('documents', []).extend(lot_documents)
    auction['status'] = 'pending.activation'
    try:
        auction['transfer_token'] = extract_lot_transfer_token(lot_doc['id'])
    except EXCEPTIONS as e:
        message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
        logger.error("Failed to extract transfer token from Lot {} ({})".format(lot_doc['id'], message))
        raise ValueError()

    context['task_instance'].xcom_push('return_value', auction)


def find_out_what_auction_create(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')

    auction_from_lot = get_next_auction(lot_doc)

    if auction_from_lot['tenderAttempts'] == 1:
        return 'create_first_english'
    else:
        return 'create_other_auction'


def create_first_english(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')
    auction = context['task_instance'].xcom_pull(task_ids='prepare_basic_data')

    auction_from_lot = get_next_auction(lot_doc)

    now_date = datetime.now(TZ)
    auction['auctionPeriod'] = auction_from_lot.get('auctionPeriod', {})
    start_date = parse_date(auction_from_lot['auctionPeriod'].get('startDate', None))

    re_obj = ACCELERATOR_RE.search(auction.get('procurementMethodDetails', ''))

    if re_obj and 'accelerator' in re_obj.groupdict():
        calc_date = calculate_business_date(
            start=now_date,
            delta=timedelta(days=20) / int(re_obj.groupdict()['accelerator']),
            context=None,
            working_days=False
        )
    else:
        calc_date = calculate_business_date(
            start=now_date,
            delta=timedelta(days=1),
            context=None,
            working_days=True
        )
    if not start_date or start_date <= calc_date:
        auction['auctionPeriod']['startDate'] = calc_date.isoformat()

    context['task_instance'].xcom_push('return_value', auction)


def create_other_auction(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')
    auction = context['task_instance'].xcom_pull(task_ids='prepare_basic_data')

    auction_from_lot = get_next_auction(lot_doc)

    now_date = datetime.now(TZ)

    auction['auctionPeriod'] = {
        'startDate': (now_date + parse_duration(auction_from_lot['tenderingDuration'])).isoformat()
    }

    context['task_instance'].xcom_push('return_value', auction)


def post_created_auction(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')

    auction_from_lot = get_next_auction(lot_doc)

    auction = get_item_from_direct_upstream_tasks(**context)

    try:
        auction = post_auction({'data': auction}, lot_doc['id'])
    except EXCEPTIONS as e:
        message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
        logger.error("Failed to create auction from Lot {} ({})".format(lot_doc['id'], message),
                     extra={'MESSAGE_ID': FAILED_AUCTION_CREATE_MESSAGE_ID})
        return 'remove_assets_from_lot'
    else:
        context['task_instance'].xcom_push('return_value', (auction, auction_from_lot['id']))
        return 'update_auction_of_lot'


def update_auction_of_lot(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document', dag_id='concierge_flow')
    auction, lot_auction_id = context['task_instance'].xcom_pull(task_ids='post_created_auction')

    data = {
        'auctionID': auction['data']['auctionID'],
        'status': 'active',
        'relatedProcessID': auction['data']['id']
    }

    patch_lot_auction(data, lot_doc['id'], lot_auction_id)


def active_salable_dag_factory(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    check_assets_active_operator = PythonOperator(
        task_id='check_assets_active',
        python_callable=check_assets_available,
        op_kwargs={'status': 'active',
                   'assets_getter': get_asset_id_from_related_process,
                   'related_lot_getter': get_asset_related_lot},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    check_auctions_availability_operator = PythonOperator(
        task_id='check_auctions_availability',
        python_callable=check_auctions_availability,
        dag=dag,
        provide_context=True
    )

    check_procurement_method_type_operator = PythonOperator(
        task_id='check_procurement_method_type',
        python_callable=check_procurement_method_type,
        dag=dag,
        provide_context=True
    )

    prepare_basic_data_operator = PythonOperator(
        task_id='prepare_basic_data',
        python_callable=prepare_basic_data,
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    find_out_what_auction_create_operator = BranchPythonOperator(
        task_id='find_out_what_auction_create',
        python_callable=find_out_what_auction_create,
        dag=dag,
        provide_context=True
    )

    create_first_english_operator = PythonOperator(
        task_id='create_first_english',
        python_callable=create_first_english,
        dag=dag,
        provide_context=True
    )

    create_other_auction_operator = PythonOperator(
        task_id='create_other_auction',
        python_callable=create_other_auction,
        dag=dag,
        provide_context=True
    )

    post_created_auction_operator = BranchPythonOperator(
        task_id='post_created_auction',
        python_callable=post_created_auction,
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    update_auction_of_lot_operator = PythonOperator(
        task_id='update_auction_of_lot',
        python_callable=update_auction_of_lot,
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    remove_assets_from_lot_operator = SubDagOperator(
        task_id='remove_assets_from_lot',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot'),
        dag=dag
    )

    patch_lot_to_composing_operator = PythonOperator(
        task_id='patch_lot_to_composing',
        python_callable=patch_lot_to,
        op_kwargs={'status': 'composing'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    check_assets_active_operator.set_downstream([check_auctions_availability_operator])

    check_auctions_availability_operator.set_downstream([check_procurement_method_type_operator])

    check_procurement_method_type_operator.set_downstream([prepare_basic_data_operator])

    prepare_basic_data_operator.set_downstream([find_out_what_auction_create_operator])

    find_out_what_auction_create_operator.set_downstream([create_first_english_operator, create_other_auction_operator])

    create_first_english_operator.set_downstream([post_created_auction_operator])
    create_other_auction_operator.set_downstream([post_created_auction_operator])

    post_created_auction_operator.set_downstream([update_auction_of_lot_operator, remove_assets_from_lot_operator])

    remove_assets_from_lot_operator.set_downstream([patch_lot_to_composing_operator])

    return dag
