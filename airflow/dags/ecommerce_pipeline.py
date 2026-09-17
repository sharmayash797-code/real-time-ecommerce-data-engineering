from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 9, 8),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_ecommerce_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=898346058562481,
    )