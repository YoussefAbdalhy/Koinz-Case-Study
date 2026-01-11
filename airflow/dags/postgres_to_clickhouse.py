from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_clickhouse_incremental",
    default_args=default_args,
    start_date=datetime(2025, 1, 9),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["spark", "postgres", "clickhouse"],
) as dag:

    spark_job = DockerOperator(
        task_id="run_spark_incremental_job",
        image="spark-job:latest",
        api_version="auto",
        auto_remove=True,
        command="""
        /opt/spark/bin/spark-submit
        --jars /opt/spark/work-dir/jars/clickhouse-jdbc-0.6.3-all.jar,/opt/spark/work-dir/jars/postgresql-42.7.6.jar
        /opt/spark/work-dir/jobs/postgres_to_clickhouse.py
        """,
        network_mode="koinz-casestudy_default",
        docker_url="unix://var/run/docker.sock",
    )

    spark_job
