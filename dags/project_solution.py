from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "goit",
}

with DAG(
    dag_id="multi_hop_datalake_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    landing_bio = BashOperator(
        task_id="landing_bio",
        bash_command="python /opt/airflow/project/landing_to_bronze.py athlete_bio",
    )

    landing_event_results = BashOperator(
        task_id="landing_event_results",
        bash_command="python /opt/airflow/project/landing_to_bronze.py athlete_event_results",
    )

    silver_bio = BashOperator(
        task_id="silver_bio",
        bash_command="python /opt/airflow/project/bronze_to_silver.py athlete_bio",
    )

    silver_event_results = BashOperator(
        task_id="silver_event_results",
        bash_command="python /opt/airflow/project/bronze_to_silver.py athlete_event_results",
    )

    gold_avg_stats = BashOperator(
        task_id="gold_avg_stats",
        bash_command="python /opt/airflow/project/silver_to_gold.py",
    )

    landing_bio >> silver_bio
    landing_event_results >> silver_event_results
    [silver_bio, silver_event_results] >> gold_avg_stats
    