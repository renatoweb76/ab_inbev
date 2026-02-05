from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

# =============================================================================
# CONFIGURATION
# =============================================================================
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['seu.email@exemplo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Paths inside the container (Mapped via Docker Volume)
SPARK_MASTER = "spark://spark-master:7077"
JAR_PATH = "/opt/airflow/jars/postgresql-42.2.18.jar"
SRC_PATH = "/opt/airflow/src"

# Template for spark-submit command
# We use 1G memory to avoid killing the container in your local test environment
SPARK_SUBMIT_CMD = f"""
/opt/spark/bin/spark-submit \
--master {SPARK_MASTER} \
--jars {JAR_PATH} \
--driver-memory 1G \
--executor-memory 1G \
"""

# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id='breweries_etl_spark',
    default_args=default_args,
    description='ETL Pipeline using Spark Submit (Bronze -> Silver -> Gold -> DW)',
    schedule_interval='0 1 * * *', # Daily at 01:00 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'etl', 'medallion'],
    # Allows passing params via UI (Trigger DAG w/ Config)
    params={"full_load": False} 
) as dag:

    # =========================================================================
    # TASK 1: BRONZE LAYER (Ingestion)
    # =========================================================================
    # Logic: Always fetches data from API.
    # If full_load=True, we could potentially fetch all history (if API supported logic),
    # but here we stick to daily snapshot logic or partitioned ingestion.
    t_bronze = BashOperator(
        task_id='bronze_layer_ingestion',
        bash_command=f"""
        {SPARK_SUBMIT_CMD} {SRC_PATH}/bronze_layer.py --date {{{{ ds }}}}
        """
    )

    # =========================================================================
    # TASK 2: SILVER LAYER (Transformation)
    # =========================================================================
    # Logic: 
    # - Incremental (Default): Processes only the folder ingestion_date=YYYY-MM-DD
    # - Full Load (Manual Trigger): Processes ALL folders (*)
    # We use Jinja templating to check the 'full_load' param.
    t_silver = BashOperator(
        task_id='silver_layer_transformation',
        bash_command=f"""
        {% if params.full_load %}
            echo "TRIGGERING FULL LOAD FOR SILVER LAYER..."
            {SPARK_SUBMIT_CMD} {SRC_PATH}/silver_layer.py
        {% else %}
            echo "RUNNING INCREMENTAL LOAD FOR DATE: {{{{ ds }}}}"
            {SPARK_SUBMIT_CMD} {SRC_PATH}/silver_layer.py --date {{{{ ds }}}}
        {% endif %}
        """
    )

    # =========================================================================
    # TASK 3: GOLD LAYER (Aggregation)
    # =========================================================================
    # Logic: Gold usually aggregates full history to ensure numbers are correct.
    t_gold = BashOperator(
        task_id='gold_layer_aggregation',
        bash_command=f"""
        {SPARK_SUBMIT_CMD} {SRC_PATH}/gold_layer.py
        """
    )

    # =========================================================================
    # TASK 4: DW LOADS (Dimensions) - Parallel Execution
    # =========================================================================
    # These scripts use mode="overwrite", so they handle Idempotency automatically.
    
    t_dim_type = BashOperator(
        task_id='load_dim_brewery_type',
        bash_command=f"{SPARK_SUBMIT_CMD} {SRC_PATH}/load_dw/load_dim_brewery_type.py"
    )

    t_dim_location = BashOperator(
        task_id='load_dim_location',
        bash_command=f"{SPARK_SUBMIT_CMD} {SRC_PATH}/load_dw/load_dim_location.py"
    )

    t_dim_name = BashOperator(
        task_id='load_dim_brewery_name',
        bash_command=f"{SPARK_SUBMIT_CMD} {SRC_PATH}/load_dw/load_dim_brewery_name.py"
    )

    # =========================================================================
    # TASK 5: DW LOAD (Fact)
    # =========================================================================
    t_fact_breweries = BashOperator(
        task_id='load_fact_breweries',
        bash_command=f"{SPARK_SUBMIT_CMD} {SRC_PATH}/load_dw/load_fact_breweries.py"
    )

    # =========================================================================
    # ORCHESTRATION FLOW
    # =========================================================================
    
    # 1. Ingest and Transform (Bronze -> Silver)
    t_bronze >> t_silver
    
    # 2. Update Analytical View (Silver -> Gold)
    t_silver >> t_gold
    
    # 3. Load Dimensions (Parallel) - Can start as soon as Silver is ready
    # Note: If dimensions depended on Gold, we would move this. 
    # But usually dimensions come from Silver (Master Data).
    t_silver >> [t_dim_type, t_dim_location, t_dim_name]
    
    # 4. Load Fact (Dependent on Gold logic OR Silver logic + Dimensions loaded)
    # Ensuring Dims are ready before Fact to avoid Foreign Key issues
    [t_dim_type, t_dim_location, t_dim_name] >> t_fact_breweries
    
    # Optional: Ensure Gold is done before Fact if Fact depends on Gold metrics, 
    # but in our script Fact reads Silver. Let's keep Gold parallel to DW load 
    # or as a dependency if needed. 
    # Current flow: Gold runs alongside DW loads.