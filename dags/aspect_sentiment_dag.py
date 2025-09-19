from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

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
    schedule_interval="@hourly",     # hourly run
    start_date=days_ago(0),
    catchup=False,                   # prevents backfill when computer wakes up
    max_active_runs=1,               # only one DAG run at a time
    concurrency=1,                   # only one task runs at a time inside the DAG
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