from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='orchestrate_breweries_pipeline',
    schedule_interval='0 2 * * *',  # executa diariamente às 2:00 AM
    catchup=False,
    default_args=default_args,
    description='Orquestra ingestão e carga DW breweries'
) as dag:

    # 1. Dispara a ingestão
    trigger_ingestion = TriggerDagRunOperator(
    task_id="trigger_ingestion_breweries",
    trigger_dag_id="ingestion_breweries",
    wait_for_completion=True,
    poke_interval=30,  # frequência com que ele checa se terminou
    reset_dag_run=True,  # recria se já existe run para essa data
    execution_date="{{ ts }}"  # usa exatamente o mesmo timestamp
)

    # 2. Sensor para garantir que a ingestão terminou (redundância, mas deixa explícito)
    wait_ingestion = ExternalTaskSensor(
        task_id='wait_ingestion_breweries',
        external_dag_id='ingestion_breweries',
        external_task_id=None,   # None espera toda a DAG terminar
        mode='poke',             # modo de espera
        poke_interval=30,        # intervalo de checagem
        timeout=60 * 60,         # até 1 hora esperando
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 3. Dispara a carga DW
    trigger_dw = TriggerDagRunOperator(
        task_id='trigger_load_dw_breweries_full',
        trigger_dag_id='load_dw_breweries_full',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 4. Sensor final para garantir que DW terminou
    wait_dw = ExternalTaskSensor(
        task_id='wait_load_dw_breweries_full',
        external_dag_id='load_dw_breweries_full',
        external_task_id=None,
        mode='poke',
        poke_interval=30,
        timeout=60 * 60,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # Encadeamento (trigger -> sensor -> trigger -> sensor)
    trigger_ingestion >> wait_ingestion >> trigger_dw >> wait_dw
