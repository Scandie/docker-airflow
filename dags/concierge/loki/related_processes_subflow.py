# -*- coding: utf-8 -*-
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from concierge.core.constants import TZ, RETRIES, RETRY_DELAY
from concierge.core.dags_utils import patch_assets_to
from concierge.core.utils import get_asset_id_from_related_process

from concierge.loki.utils import (
    _clean_asset_related_processes,
    get_assets_related_processes,
    _add_related_process_to_assets
)


def push_marker(key, value, **context):
    context['task_instance'].xcom_push(key, value)


def add_related_process_to_assets(on_success, on_fail, **context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    result, patched_rPs = _add_related_process_to_assets(lot_doc)
    context['task_instance'].xcom_push('patched_rPs', patched_rPs)

    if not result:
        return on_fail
    return on_success


def clean_asset_related_processes(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    related_processes = get_assets_related_processes(lot_doc)

    if related_processes:
        result = _clean_asset_related_processes(related_processes)
        if result is False:
            raise ValueError('Can`t clean up asset.related_processes')
            # TODO 'log broken lot' ??


def remove_assets_from_lot(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    remove_lot_related_processes_from_assets_operator = PythonOperator(
        task_id='remove_lot_related_processes_from_assets',
        python_callable=clean_asset_related_processes,
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    repatch_assets_to_pending_operator = PythonOperator(
        task_id='repatch_assets_to_pending',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'pending',
                   'lot_id': False,
                   'assets_getter': get_asset_id_from_related_process,
                   'message': 'Assets {} will be repatched to {}'},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    repatch_assets_to_pending_operator.set_downstream(remove_lot_related_processes_from_assets_operator)

    return dag


def add_assets_to_lot(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    repatch_assets_because_of_failed_adding_to_lot_operator = PythonOperator(
        task_id='repatch_assets_to_pending',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'pending',
                   'lot_id': False,
                   'assets_subset': True,
                   'message': 'Assets {} will be repatched to {}'},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    remove_assets_from_lot_operator = SubDagOperator(
        task_id='remove_assets_from_lot',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot'),
        dag=dag
    )

    add_assets_to_lot_success_operator = PythonOperator(
        task_id='add_assets_to_lot.success',
        python_callable=push_marker,
        op_kwargs={'key': 'lots_added', 'value': True},
        provide_context=True
    )

    add_assets_to_lot_fail_operator = PythonOperator(
        task_id='add_assets_to_lot.fail',
        python_callable=push_marker,
        op_kwargs={'key': 'lots_added', 'value': False},
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    add_related_process_to_assets_operator = BranchPythonOperator(
        task_id='add_assets_to_lot.related_processes',
        python_callable=add_related_process_to_assets,
        op_kwargs={'on_success': add_assets_to_lot_success_operator.task_id,
                   'on_fail': remove_assets_from_lot_operator.task_id},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_assets_to_verification_operator = BranchPythonOperator(
        task_id='add_assets_to_lot.patch_status',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'verification',
                   'on_fail': repatch_assets_because_of_failed_adding_to_lot_operator.task_id,
                   'on_success': add_related_process_to_assets_operator.task_id,
                   'assets_getter': get_asset_id_from_related_process},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_assets_to_verification_operator.set_downstream([
        repatch_assets_because_of_failed_adding_to_lot_operator,
        add_related_process_to_assets_operator
    ])

    add_related_process_to_assets_operator.set_downstream([
        add_assets_to_lot_success_operator,
        remove_assets_from_lot_operator
    ])

    remove_assets_from_lot_operator.set_downstream(
        add_assets_to_lot_fail_operator
    )

    repatch_assets_because_of_failed_adding_to_lot_operator.set_downstream(
        add_assets_to_lot_fail_operator
    )

    return dag
