# -*- coding: utf-8 -*-
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from concierge.core.constants import TZ, RETRIES, RETRY_DELAY
from concierge.core.dags_utils import patch_assets_to, patch_lot_to, check_assets_available, release_lot


def verification_dag(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    patch_lot_to_active_salable_operator = PythonOperator(
        task_id='patch_lot_to_active_salable',
        python_callable=patch_lot_to,
        op_kwargs={'status': 'active.salable'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_assets_to_pending_after_verification_operator = PythonOperator(
        task_id='patch_assets_to_pending_after_verification',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'pending',
                   'lot_id': None,
                   'assets_subset': True,
                   'message': 'Assets {} will be repatched to {}'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_assets_to_pending_after_active_operator = PythonOperator(
        task_id='patch_assets_to_pending_after_active',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'pending',
                   'lot_id': None,
                   'assets_subset': True,
                   'message': 'Assets {} will be repatched to {}'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_assets_to_active_operator = BranchPythonOperator(
        task_id='patch_assets_to_active',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'active',
                   'lot_id': True,
                   'on_fail': patch_assets_to_pending_after_active_operator.task_id,
                   'on_success': patch_lot_to_active_salable_operator.task_id},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    add_assets_to_lot_operator = BranchPythonOperator(
        task_id='add_assets_to_lot',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'verification',
                   'lot_id': True,
                   'on_fail': patch_assets_to_pending_after_verification_operator.task_id,
                   'on_success': patch_assets_to_active_operator.task_id},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_lot_to_pending_operator = PythonOperator(
        task_id='patch_lot_to_pending',
        python_callable=patch_lot_to,
        op_kwargs={'status': 'pending'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    check_assets_available_operator = BranchPythonOperator(
        task_id='check_assets_available',
        python_callable=check_assets_available,
        op_kwargs={'on_fail': patch_lot_to_pending_operator.task_id,
                   'on_success': add_assets_to_lot_operator.task_id},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    check_assets_available_operator.set_downstream([
        add_assets_to_lot_operator,
        patch_lot_to_pending_operator
    ])

    add_assets_to_lot_operator.set_downstream([
        patch_assets_to_pending_after_verification_operator,
        patch_assets_to_active_operator
    ])

    patch_assets_to_active_operator.set_downstream([
        patch_lot_to_active_salable_operator,
        patch_assets_to_pending_after_active_operator
    ])

    return dag
