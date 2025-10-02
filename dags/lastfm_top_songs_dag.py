import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.constants import (
    AirflowConfig,
    SparkConfig,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="kraken_top_songs",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    validate = SparkSubmitOperator(
        task_id="validate",
        application=AirflowConfig.VALIDATE_SCRIPT.value,
        conn_id=None,  # run locally inside container
        master=SparkConfig.SPARK_MASTER.value,
        conf=SparkConfig.SPARK_CONF.value,
        env_vars={
            AirflowConfig.LASTFM_DIR_ENV.value: AirflowConfig.DEFAULT_LASTFM_DIR.value,
        },
    )

    compute = SparkSubmitOperator(
        task_id="compute",
        application=AirflowConfig.COMPUTE_SCRIPT.value,
        conn_id=None,
        master=SparkConfig.SPARK_MASTER.value,
        conf=SparkConfig.SPARK_CONF.value,
        env_vars={
            AirflowConfig.LASTFM_DIR_ENV.value: AirflowConfig.DEFAULT_LASTFM_DIR.value,
            AirflowConfig.OUTPUT_DIR_ENV.value: AirflowConfig.DEFAULT_OUTPUT_DIR.value,
        },
    )

    validate >> compute


