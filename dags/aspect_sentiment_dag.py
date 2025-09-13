from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "aspect_sentiment_pipeline",
    default_args=default_args,
    description="Ingest Reddit → Process → Export for Power BI",
    schedule_interval="@hourly",  # run once daily (change to @hourly / cron as needed)
    start_date=datetime(2025, 9, 12),
    catchup=False,
    tags=["aspect-sentiment", "portfolio"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_reddit",
        bash_command="python /opt/airflow/realtime/ingest_reddit_stream.py",
    )

    process = BashOperator(
        task_id="process_reviews",
        bash_command="python /opt/airflow/realtime/process_new_phase3.py",
    )

    export = BashOperator(
        task_id="export_for_powerbi",
        bash_command="python /opt/airflow/tools/export_for_powerbi.py",
    )

    ingest >> process >> export