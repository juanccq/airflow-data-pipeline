from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": ["data-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver_spark_etl",
    default_args=default_args,
    description="Event-driven Spark ETL triggered via SSH",
    schedule="@daily",
    start_date=datetime(2026, 10, 14),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "rustfs", "data_lake"],
) as dag:
    wait_for_raw_data = S3KeySensor(
        task_id="wait_for_transaction_logs",
        bucket_name="spark-data",
        bucket_key="transaction_logs_{{ ds_nodash }}.json",
        aws_conn_id="rustfs_s3_conn",
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    trigger_spark_cluster = SSHOperator(
        task_id="submit_spark_join_job",
        ssh_conn_id="spark_master_ssh",
        command="""
            source /home/sparkuser/.venv/bin/activate && \
            /home/sparkuser/spark/bin/spark-submit \
            --packages org.apache.hadoop:hadoop-aws:3.3.2 \
            /home/sparkuser/spark_total_by_city_workers.py {{ ds_nodash }}
        """,
        cmd_timeout=3600,
        get_pty=True,
    )

    spark_packages = (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0"
    )

    load_gold_warehouse = SSHOperator(
        task_id="load_postgres_staging",
        ssh_conn_id="spark_master_ssh",
        command=f"""
            /home/sparkuser/spark/bin/spark-submit \
            --master local[*] \
            --packages {spark_packages} \
            /home/sparkuser/spark_silver_to_gold.py {{{{ ds_nodash }}}}
        """,
        cmd_timeout=3600,
        get_pty=True,
    )

    clear_staging_cmd = (
        "docker exec gold_data_warehouse psql -U dw_admin -d gold_zone "
        "-c \"DELETE FROM staging.sales_by_city WHERE load_date = '{{ ds_nodash }}';\""
    )

    clear_staging_partition = SSHOperator(
        task_id="clear_staging_partition",
        ssh_conn_id="airflow_host_ssh",
        command=clear_staging_cmd,
    )

    dbt_cmd = (
        "source /home/airflow/dbt-env/bin/activate && "
        "cd /home/airflow/airflow/gold_models && "
        "dbt run && dbt test"
    )

    run_dbt_transform = SSHOperator(
        task_id="run_dbt_models",
        ssh_conn_id="airflow_host_ssh",
        command=dbt_cmd,
        cmd_timeout=300,
    )

    (
        wait_for_raw_data
        >> trigger_spark_cluster
        >> clear_staging_partition
        >> load_gold_warehouse
        >> run_dbt_transform
    )
