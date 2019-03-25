# -*- coding: utf-8 -*-
import logging
import os
from datetime import timedelta

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators import TriggerMultiDagRunOperator
from airflow.utils.dates import days_ago

from concierge.core.config_validation import concierge
from concierge.core.constants import DEFAULTS, CONFIG_AIRFLOW_VAR, SKIP_LOT_MESSAGE_ID
from concierge.core.listener import Concierge

logger = logging.getLogger('openregistry.concierge.worker')

default_args = {
    'owner': 'airflow',
}

trigger_dag = DAG(
    dag_id='trigger_concierge_flow',
    max_active_runs=1,
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(1),
    default_args=default_args,
)


def generate_dag_run(context, logger, *args, **kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open('{}/../../configs/concierge.yaml'.format(current_dir)) as config_object:  # TODO add config getter
        config = yaml.load(config_object.read())

    # config validation
    config = concierge(config)

    DEFAULTS.update(config)
    Variable.set(CONFIG_AIRFLOW_VAR, DEFAULTS, serialize_json=True)

    worker = Concierge(DEFAULTS)

    for lot in worker.get_lot():
        if not worker.created_clients['lots_mapping'].has(lot['id']):
            if worker.created_clients['lots_mapping'].type == 'redis':
                worker.created_clients['lots_mapping'].put(str(lot['id']), True)
            yield context['dro'](
                payload={'lot': lot},
                run_id='lot_{}'.format(lot['id'])
            )
        else:
            logger.info("Skipping Lot {} (Already processing)".format(
                lot['id']), extra={'MESSAGE_ID': SKIP_LOT_MESSAGE_ID}
            )


gen_target_dag_run = TriggerMultiDagRunOperator(
    task_id='gen_target_dag_run',
    dag=trigger_dag,
    trigger_dag_id='concierge_flow',
    python_callable=generate_dag_run,
    op_kwargs={'logger': logger}  # due to globals overriding to None
)
