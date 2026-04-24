from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
from config.settings import S3_BRONZE_BUCKET
from config.settings import SPARK_REPO_PATH
from config.settings import SPARK_SUBMIT_BIN
from config.settings import SPARK_SUBMIT_EXTRA_ARGS
from config.settings import SPARK_SSH_CONN_ID



with DAG(
    dag_id="bronze_manual_support_v1",
    description="DAG manual de soporte para ingesta y validacion Bronze",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 19, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["green-ai", "bronze", "support", "manual"],
) as dag:
    ingesta_electricity_maps = BashOperator(
        task_id="ingest_electricity_maps_latest",
        bash_command=(
            "mkdir -p /opt/airflow/bronze/reference && "
            "cp /opt/airflow/data/geo_cloud_to_country_and_zones.csv "
            "/opt/airflow/bronze/reference/ && "
            "python3 /opt/airflow/scripts/ingest_electricity_maps.py "
            "--mode latest "
            f"--bucket {S3_BRONZE_BUCKET}"
        ),
    )

    validate_bronze = SSHOperator(
        task_id="validate_bronze_quality_remote",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/bronze_validations.py"
        ),
        cmd_timeout=1800,
    )

    ingesta_electricity_maps >> validate_bronze
