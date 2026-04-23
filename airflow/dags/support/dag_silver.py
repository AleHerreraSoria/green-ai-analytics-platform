from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
import os

SPARK_SSH_CONN_ID = "spark_ssh"
SPARK_REPO_PATH = os.getenv("SPARK_REPO_PATH", "/opt/green-ai-analytics-platform/spark")

with DAG(
    dag_id="silver_manual_support_v1",
    description="DAG manual de soporte para transformacion y validacion Silver",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 19, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["green-ai", "silver", "support", "manual", "spark-remote"],
) as dag:
    process_silver = SSHOperator(
        task_id="spark_silver_transform_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            "export S3_BRONZE_BUCKET=${S3_BRONZE_BUCKET:-green-ai-pf-bronze-a0e96d06} "
            "S3_SILVER_BUCKET=${S3_SILVER_BUCKET:-green-ai-pf-silver-a0e96d06} && "
            "spark-submit jobs/etl/bronze_to_silver.py"
        ),
        cmd_timeout=3600,
    )

    validate_silver = SSHOperator(
        task_id="spark_silver_validations_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            "export S3_BRONZE_BUCKET=${S3_BRONZE_BUCKET:-green-ai-pf-bronze-a0e96d06} "
            "S3_SILVER_BUCKET=${S3_SILVER_BUCKET:-green-ai-pf-silver-a0e96d06} && "
            "spark-submit jobs/quality/silver_validations.py"
        ),
        cmd_timeout=3600,
    )

    process_silver >> validate_silver