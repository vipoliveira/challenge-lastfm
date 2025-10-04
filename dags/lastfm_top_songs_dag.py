from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.constants import AirflowConfig
from src.pipelines.lastfm.entrypoint import Entrypoint


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="lastfm_top_songs",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    validate = PythonOperator(
        task_id="validate",
        python_callable=Entrypoint.validate_task,
        op_kwargs={
            "input_dir": AirflowConfig.DEFAULT_INPUT_DIR.value,
        },
    )

    compute = PythonOperator(
        task_id="compute",
        python_callable=Entrypoint.compute_task,
        op_kwargs={
            "input_dir": AirflowConfig.DEFAULT_INPUT_DIR.value,
            "events_output_dir": AirflowConfig.EVENTS_OUTPUT_DIR.value,
            "profiles_output_dir": AirflowConfig.PROFILES_OUTPUT_DIR.value,
        },
    )
    
    post_process = PythonOperator(
        task_id="post_process",
        python_callable=Entrypoint.post_process_validation,
        op_kwargs={
            "events_path": AirflowConfig.EVENTS_OUTPUT_DIR.value,
            "profiles_path": AirflowConfig.PROFILES_OUTPUT_DIR.value,
            "output_dir": AirflowConfig.RESULTS_OUTPUT_DIR.value,
        },
    )

    compute >> validate >> post_process


