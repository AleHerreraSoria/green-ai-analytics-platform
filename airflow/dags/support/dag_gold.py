from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
import os


SPARK_SSH_CONN_ID = "spark_ssh"
SPARK_REPO_PATH = os.getenv("SPARK_REPO_PATH", "/opt/green-ai-analytics-platform/spark")
S3_SILVER = os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")
S3_GOLD = os.getenv("S3_GOLD_BUCKET", "green-ai-pf-gold-a0e96d06")


with DAG(
    dag_id="gold_manual_support_v1",
    description="DAG manual de soporte para ejecutar Gold en Spark remoto",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 19, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["green-ai", "gold", "support", "manual", "spark-remote"],
) as dag:
    build_gold_remote = SSHOperator(
        task_id="build_gold_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_SILVER_BUCKET={S3_SILVER} S3_GOLD_BUCKET={S3_GOLD} && "
            "spark-submit jobs/etl/silver_to_gold.py"
        ),
        cmd_timeout=7200,
    )

    validate_gold_remote = SSHOperator(
        task_id="validate_gold_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_GOLD_BUCKET={S3_GOLD} && "
            "spark-submit jobs/quality/gold_validations.py --fail"
        ),
        cmd_timeout=3600,
    )

    build_gold_remote >> validate_gold_remote
