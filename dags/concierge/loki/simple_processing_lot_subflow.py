# -*- coding: utf-8 -*-
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from concierge.core.constants import TZ, RETRIES, RETRY_DELAY
from concierge.core.dags_utils import patch_assets_to, patch_lot_to, release_lot
from concierge.core.utils import get_asset_id_from_related_process

from concierge.loki.related_processes_subflow import remove_assets_from_lot

asset_status_depends_lot = {
    'pending.dissolution': 'pending',
    'pending.deleted': 'pending',
    'pending.sold': 'complete',
}
lot_statuses_flow = {
    'pending.dissolution': 'dissolved',
    'pending.deleted': 'deleted',
    'pending.sold': 'sold'
}


def check_next_asset_status(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')
    next_asset_status = asset_status_depends_lot[lot_doc['status']]
    if next_asset_status == 'pending':
        return 'remove_assets_from_lot'
    else:
        return 'simple_processing_patch_asset_status'


def patch_assets_status(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')
    return patch_assets_to(
        asset_status_depends_lot[lot_doc['status']],
        lot_doc, False,
        assets_getter=get_asset_id_from_related_process,
        **context
    )


def patch_lot_status(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')
    return patch_lot_to(lot_statuses_flow[lot_doc['status']], lot_doc, **context)


def simple_lot_processing_dag(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    check_next_asset_status_operator = BranchPythonOperator(
        task_id='check_next_asset_status',
        python_callable=check_next_asset_status,
        provide_context=True,
        dag=dag
    )

    patch_asset_status_operator = PythonOperator(
        task_id='simple_processing_patch_asset_status',
        python_callable=patch_assets_status,
        provide_context=True,
        dag=dag,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )
    patch_lot_status_operator = PythonOperator(
        task_id='simple_processing_patch_lot_status',
        python_callable=patch_lot_status,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    remove_assets_from_lot_operator = SubDagOperator(
        task_id='remove_assets_from_lot',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot'),
        dag=dag,
    )

    check_next_asset_status_operator.set_downstream([
        patch_asset_status_operator,
        remove_assets_from_lot_operator
    ])

    remove_assets_from_lot_operator.set_downstream(patch_lot_status_operator)

    patch_asset_status_operator.set_downstream(patch_lot_status_operator)

    return dag
