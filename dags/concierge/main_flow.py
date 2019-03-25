# -*- coding: utf-8 -*-
import logging
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from concierge.core.constants import (
    LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR, SKIP_LOT_MESSAGE_ID,
    PROCESS_BASIC_LOT_MESSAGE_ID, PROCESS_LOKI_LOT_MESSAGE_ID
)
from concierge.core.utils import check_lot

from concierge.basic.verification_status_processing import verification_dag
from concierge.basic.simple_processing_lot_subflow import simple_lot_processing_dag

from concierge.loki.verification_status_processing import verification_dag as loki_verification_dag
from concierge.loki.active_salable_status_processing import active_salable_dag_factory as loki_active_salable_dag_factory
from concierge.loki.simple_processing_lot_subflow import simple_lot_processing_dag as simple_loki_lot_processing_dag


logger = logging.getLogger('openregistry.concierge.worker')

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'concierge_flow',
    default_args=default_args,
    schedule_interval=None,
)


def get_document(**context):
    lot_doc = context['dag_run'].conf['lot']
    context['task_instance'].xcom_push('return_value', lot_doc)


def check_lot_type(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document')
    lot_type_configurator = Variable.get(LOT_TYPE_CONFIGURATOR_AIRFLOW_VAR, deserialize_json=True)

    if lot_doc['lotType'] not in lot_type_configurator:
        logger.warning('Such lotType %s is not supported by this concierge configuration' % lot_doc['lotType'])
        return

    return 'check_{}_availability'.format(lot_type_configurator[lot_doc['lotType']])


def check_basic_availability(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document')

    lot_available = check_lot(lot_doc, 'basic')
    if not lot_available:
        logger.info("Skipping Lot {}".format(lot_doc['id']), extra={'MESSAGE_ID': SKIP_LOT_MESSAGE_ID})
        return

    logger.info("Processing Lot {} in status {}".format(
        lot_doc['id'], lot_doc['status']), extra={'MESSAGE_ID': PROCESS_BASIC_LOT_MESSAGE_ID}
    )

    if lot_doc['status'] in ['verification']:
        return 'process_verification_lot'
    else:
        return 'process_simple_status_lot'


def check_loki_availability(**context):
    lot_doc = context['task_instance'].xcom_pull(task_ids='get_document')

    lot_available = check_lot(lot_doc, 'loki')
    if not lot_available:
        logger.info("Skipping Lot {}".format(lot_doc['id']), extra={'MESSAGE_ID': SKIP_LOT_MESSAGE_ID})
        return

    logger.info("Processing Lot {} in status {}".format(
        lot_doc['id'], lot_doc['status']), extra={'MESSAGE_ID': PROCESS_LOKI_LOT_MESSAGE_ID}
    )

    if lot_doc['status'] == 'verification':
        return 'process_loki_verification_lot'
    elif lot_doc['status'] == 'active.salable':
        return 'process_loki_active_salable_lot'
    else:
        return 'process_simple_status_loki_lot'


# Flow Definition
get_doc = PythonOperator(
    task_id='get_document',
    python_callable=get_document,
    dag=dag,
    provide_context=True
)

check_lot_type_operator = BranchPythonOperator(
    task_id='check_lot_type',
    python_callable=check_lot_type,
    dag=dag,
    provide_context=True,
)

check_basic_availability_operator = BranchPythonOperator(
    task_id='check_basic_availability',
    python_callable=check_basic_availability,
    dag=dag,
    provide_context=True,
    retries=5,
    retry_delay=timedelta(seconds=30)
)

check_loki_availability_operator = BranchPythonOperator(
    task_id='check_loki_availability',
    python_callable=check_loki_availability,
    dag=dag,
    provide_context=True,
    retries=5,
    retry_delay=timedelta(seconds=30)
)

verification_lot_processing_operator = SubDagOperator(
    task_id='process_verification_lot',
    subdag=verification_dag(dag.dag_id, 'process_verification_lot'),
    dag=dag,
    retries=5,
    retry_delay=timedelta(seconds=30)
)

simple_lot_processing_operator = SubDagOperator(
    task_id='process_simple_status_lot',
    subdag=simple_lot_processing_dag(dag.dag_id, 'process_simple_status_lot'),
    dag=dag
)

verification_loki_lot_processing_operator = SubDagOperator(
    task_id='process_loki_verification_lot',
    subdag=loki_verification_dag(dag.dag_id, 'process_loki_verification_lot'),
    dag=dag
)
active_salable_loki_lot_processing_operator = SubDagOperator(
    task_id='process_loki_active_salable_lot',
    subdag=loki_active_salable_dag_factory(dag.dag_id, 'process_loki_active_salable_lot'),
    dag=dag
)
process_simple_status_loki_lot_operator = SubDagOperator(
    task_id='process_simple_status_loki_lot',
    subdag=simple_loki_lot_processing_dag(dag.dag_id, 'process_simple_status_loki_lot'),
    dag=dag
)

get_doc.set_downstream(check_lot_type_operator)

check_lot_type_operator.set_downstream([
    check_basic_availability_operator, check_loki_availability_operator
])

check_basic_availability_operator.set_downstream([
    verification_lot_processing_operator, simple_lot_processing_operator
])

check_loki_availability_operator.set_downstream([
    verification_loki_lot_processing_operator,
    active_salable_loki_lot_processing_operator,
    process_simple_status_loki_lot_operator
])
