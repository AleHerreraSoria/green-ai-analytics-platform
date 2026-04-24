from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
from config.settings import S3_GOLD_BUCKET
from config.settings import S3_SILVER_BUCKET
from config.settings import SPARK_REPO_PATH
from config.settings import SPARK_SUBMIT_BIN
from config.settings import SPARK_SUBMIT_EXTRA_ARGS
from config.settings import SPARK_SSH_CONN_ID


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
            f"export S3_SILVER_BUCKET={S3_SILVER_BUCKET} S3_GOLD_BUCKET={S3_GOLD_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/etl/silver_to_gold.py"
        ),
        cmd_timeout=7200,
    )

    validate_gold_remote = SSHOperator(
        task_id="validate_gold_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_GOLD_BUCKET={S3_GOLD_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/gold_validations.py --fail"
        ),
        cmd_timeout=3600,
    )

    build_gold_remote >> validate_gold_remote
