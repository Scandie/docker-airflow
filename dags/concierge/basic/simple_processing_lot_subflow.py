# -*- coding: utf-8 -*-
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from concierge.core.constants import TZ, RETRIES, RETRY_DELAY
from concierge.core.dags_utils import patch_assets_to, patch_lot_to, release_lot

asset_status_depends_lot = {
    'recomposed': 'pending',
    'pending.dissolution': 'pending',
    'pending.deleted': 'pending',
    'pending.sold': 'complete',
}
lot_statuses_flow = {
    'recomposed': 'pending',
    'pending.dissolution': 'dissolved',
    'pending.deleted': 'deleted',
    'pending.sold': 'sold'
}


def patch_assets_status(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')
    next_asset_status = asset_status_depends_lot[lot_doc['status']]
    lot_id = None if next_asset_status == 'pending' else True
    return patch_assets_to(next_asset_status, lot_doc, lot_id, **context)


def patch_lot_status(**context):
    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')
    return patch_lot_to(lot_statuses_flow[lot_doc['status']], lot_doc, **context)


def simple_lot_processing_dag(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
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
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_asset_status_operator.set_downstream(patch_lot_status_operator)

    return dag
