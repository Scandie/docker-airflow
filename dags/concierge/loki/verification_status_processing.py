# -*- coding: utf-8 -*-
from copy import deepcopy
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from concierge.core.constants import TZ, RETRIES, RETRY_DELAY
from concierge.core.dags_utils import (
    patch_lot_to, patch_assets_to, release_lot,
    check_assets_available, patch_related_processes,
)
from concierge.core.utils import (
    get_client_from_zodb, patch_lot,
    get_asset_id_from_related_process,
    get_asset_related_lot
)

from concierge.loki.related_processes_subflow import add_assets_to_lot, remove_assets_from_lot


KEYS_FOR_LOKI_PATCH = {
    'title': 'title',
    'title_ru': 'title_ru',
    'title_en': 'title_en',
    'description': 'description',
    'description_ru': 'description_ru',
    'description_en': 'description_en',
    'assetHolder': 'lotHolder',
    'items': 'items',
    'assetCustodian': 'lotCustodian',
}


def check_result_of_adding_asset_to_lot(on_success, on_fail, **context):
    add_assets_to_lot_task_id = 'add_assets_to_lot'

    dag_id = '{}.{}'.format(
        context['task_instance'].dag_id,
        add_assets_to_lot_task_id,
    )

    results = context['task_instance'].xcom_pull(
        dag_id=dag_id,
        task_ids=[task.format(add_assets_to_lot_task_id) for task in ('{}.success', '{}.fail')],
        key='lots_added'
    )
    result = [result for result in results if result is not None][0]

    if result:
        return on_success
    return on_fail


def patch_lot_with_asset_data_to_pending_status(**context):
    assets_client = get_client_from_zodb('concierge', 'assets_client')

    lot_doc = context['task_instance'].xcom_pull(dag_id='concierge_flow', task_ids='get_document')

    lot_assets_related_processes = [
        rP for rP in lot_doc.get('relatedProcesses', [])
        if rP['type'] == 'asset'
    ]
    asset = assets_client.get_asset(lot_assets_related_processes[0]['relatedProcessID']).data

    to_patch = {l_key: asset.get(a_key) for a_key, l_key in KEYS_FOR_LOKI_PATCH.items()}
    to_patch['decisions'] = deepcopy(lot_doc['decisions'])

    for dec in deepcopy(asset['decisions']):
        dec.update(
            {'relatedItem': asset['id']}
        )
        to_patch['decisions'].append(dec)

    result = patch_lot(lot_doc, 'pending', to_patch)

    if result is False:
        return 'remove_identifiers_from_assets_related_processes_after_pending_failed'
    else:
        return 'patch_lot_with_asset_data_to_pending_status.success'


def verification_dag(parent_dag_id, child_dag_id):
    dag = DAG(
        '{}.{}'.format(parent_dag_id, child_dag_id),
        start_date=datetime.now(TZ),
        schedule_interval=None,
    )

    patch_lot_with_asset_data_to_pending_status_operator = BranchPythonOperator(
        task_id='patch_lot_with_asset_data_to_pending_status',
        python_callable=patch_lot_with_asset_data_to_pending_status,
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_lot_to_invalid_operator = PythonOperator(
        task_id='patch_lot_to_invalid',
        python_callable=patch_lot_to,
        op_kwargs={'status': 'invalid'},
        dag=dag,
        provide_context=True,
        on_success_callback=release_lot,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
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

    clean_lot_asset_type_related_process_after_patching_rps_failed = PythonOperator(
        task_id='remove_identifiers_from_assets_related_processes',
        python_callable=patch_related_processes,
        op_kwargs={'patched_rPS': True,
                   'cleanup': True},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_lot_asset_type_related_process_operator = BranchPythonOperator(
        task_id='add_identifiers_to_assets_related_processes',
        python_callable=patch_related_processes,
        op_kwargs={'on_fail': clean_lot_asset_type_related_process_after_patching_rps_failed.task_id,
                   'on_success': patch_lot_with_asset_data_to_pending_status_operator.task_id},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    remove_assets_from_lot_after_active_failed_operator = SubDagOperator(
        task_id='remove_assets_from_lot_after_active_failed',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot_after_active_failed'),
        dag=dag,
        on_success_callback=release_lot
    )

    patch_assets_to_active_operator = BranchPythonOperator(
        task_id='patch_assets_to_active',
        python_callable=patch_assets_to,
        op_kwargs={'status': 'active',
                   'on_fail': remove_assets_from_lot_after_active_failed_operator.task_id,
                   'on_success': patch_lot_asset_type_related_process_operator.task_id,
                   'assets_getter': get_asset_id_from_related_process},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    remove_identifiers_from_assets_related_processes_after_pending_failed_operator = PythonOperator(
        task_id='remove_identifiers_from_assets_related_processes_after_pending_failed',
        python_callable=patch_related_processes,
        op_kwargs={'cleanup': True},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    add_assets_to_lot_operator = SubDagOperator(
        task_id='add_assets_to_lot',
        subdag=add_assets_to_lot(dag.dag_id, 'add_assets_to_lot'),
        dag=dag
    )

    check_assets_available_operator = BranchPythonOperator(
        task_id='check_assets_available',
        python_callable=check_assets_available,
        op_kwargs={'on_fail': patch_lot_to_invalid_operator.task_id,
                   'on_success': add_assets_to_lot_operator.task_id,
                   'assets_getter': get_asset_id_from_related_process,
                   'related_lot_getter': get_asset_related_lot},
        dag=dag,
        provide_context=True,
        retries=RETRIES,
        retry_delay=RETRY_DELAY
    )

    patch_lot_with_asset_data_to_pending_status_success_operator = DummyOperator(
        task_id='{}.success'.format(
            patch_lot_with_asset_data_to_pending_status_operator.task_id
        ),
        on_success_callback=release_lot
    )

    add_assets_to_lot_fail_operator = DummyOperator(
        task_id='{}.fail'.format(add_assets_to_lot_operator.task_id),
        on_success_callback=release_lot
    )

    check_result_of_adding_asset_to_lot_operator = BranchPythonOperator(
        task_id='check_result_of_adding_asset_to_lot',
        python_callable=check_result_of_adding_asset_to_lot,
        op_kwargs={'on_fail': add_assets_to_lot_fail_operator.task_id,
                   'on_success': patch_assets_to_active_operator.task_id},
        dag=dag,
        provide_context=True
    )

    remove_assets_from_lot_after_patching_rps_failed_operator = SubDagOperator(
        task_id='remove_assets_from_lot_after_patching_rps_failed',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot_after_patching_rps_failed'),
        dag=dag,
        on_success_callback=release_lot
    )

    remove_assets_from_lot_after_pending_failed_operator = SubDagOperator(
        task_id='remove_assets_from_lot_after_pending_failed',
        subdag=remove_assets_from_lot(dag.dag_id, 'remove_assets_from_lot_after_pending_failed'),
        dag=dag
    )

    check_assets_available_operator.set_downstream([
        add_assets_to_lot_operator,
        patch_lot_to_invalid_operator
    ])

    add_assets_to_lot_operator.set_downstream(check_result_of_adding_asset_to_lot_operator)

    check_result_of_adding_asset_to_lot_operator.set_downstream([
        add_assets_to_lot_fail_operator,
        patch_assets_to_active_operator
    ])

    patch_assets_to_active_operator.set_downstream([
        remove_assets_from_lot_after_active_failed_operator,
        patch_lot_asset_type_related_process_operator
    ])

    patch_lot_asset_type_related_process_operator.set_downstream([
        clean_lot_asset_type_related_process_after_patching_rps_failed,
        patch_lot_with_asset_data_to_pending_status_operator
    ])

    clean_lot_asset_type_related_process_after_patching_rps_failed.set_downstream(
        remove_assets_from_lot_after_patching_rps_failed_operator
    )

    patch_lot_with_asset_data_to_pending_status_operator.set_downstream([
        remove_identifiers_from_assets_related_processes_after_pending_failed_operator,
        patch_lot_with_asset_data_to_pending_status_success_operator
    ])

    remove_identifiers_from_assets_related_processes_after_pending_failed_operator.set_downstream(
        remove_assets_from_lot_after_pending_failed_operator
    )

    remove_assets_from_lot_after_pending_failed_operator.set_downstream(
        patch_lot_to_composing_operator
    )

    return dag
