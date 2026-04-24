from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import os
import pendulum
from config.settings import AWS_CONN_ID
from config.settings import S3_BRONZE_BUCKET
from config.settings import S3_GOLD_BUCKET
from config.settings import S3_SILVER_BUCKET
from config.settings import S3_SOURCES_BUCKET
from config.settings import SCRIPTS_PATH
from config.settings import SPARK_REPO_PATH
from config.settings import SPARK_SUBMIT_BIN
from config.settings import SPARK_SUBMIT_EXTRA_ARGS
from config.settings import SPARK_SSH_CONN_ID


def copy_sources_bucket_to_bronze():
    """
    Copia datasets del bucket de fuentes (layout actual del negocio) al bucket Bronze
    con las claves que esperan Spark y el resto del DAG (layout legado).
    """
    from botocore.exceptions import ClientError
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = hook.get_conn()

    # (clave en S3_SOURCES_BUCKET, clave en S3_BRONZE_BUCKET)
    mapping = [
        ("Code_Carbon/gpus.csv", "mlco2/gpus.csv"),
        ("Code_Carbon/impact.csv", "mlco2/impact.csv"),
        ("Code_Carbon/instances.csv", "mlco2/instances.csv"),
        ("Code_Carbon/2021-10-27yearly_averages.csv", "mlco2/2021-10-27yearly_averages.csv"),
        (
            "Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv",
            "global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv",
        ),
        ("Our_World_In_Data/owid-energy-data.csv", "owid/owid-energy-data.csv"),
        ("reference/geo_cloud_to_country_and_zones.csv", "reference/geo_cloud_to_country_and_zones.csv"),
        (
            "World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
            "world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
        ),
        (
            "World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
            "world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
        ),
    ]

    for src_key, dest_key in mapping:
        try:
            client.head_object(Bucket=S3_SOURCES_BUCKET, Key=src_key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise ValueError(
                    f"Objeto fuente no encontrado: s3://{S3_SOURCES_BUCKET}/{src_key}"
                ) from e
            raise
        client.copy_object(
            Bucket=S3_BRONZE_BUCKET,
            Key=dest_key,
            CopySource={"Bucket": S3_SOURCES_BUCKET, "Key": src_key},
        )
        print(f"OK s3://{S3_SOURCES_BUCKET}/{src_key} -> s3://{S3_BRONZE_BUCKET}/{dest_key}")

# ==========================================================
# DEFINICIÓN DEL DAG
# ==========================================================

with DAG(
    dag_id='ingesta_full_pipeline_v3_gold',
    description='Pipeline E2E principal Bronze -> Silver -> Gold',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 4, 21, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['green-ai', 'e2e', 'gold-kimball'],
    default_args={'retries': 1, 'retry_delay': pendulum.duration(minutes=5)}
) as dag:

    # --- FASE 1: BRONZE ---
    task_sources_to_bronze = PythonOperator(
        task_id='sync_sources_bucket_to_bronze',
        python_callable=copy_sources_bucket_to_bronze,
    )

    task_build_aws_pricing_reference = BashOperator(
        task_id='build_aws_ec2_pricing_reference',
        bash_command=f"python3 {SCRIPTS_PATH}/build_aws_ec2_pricing_reference.py",
    )

    task_generate_logs = BashOperator(
        task_id='generate_usage_logs',
        bash_command=(
            f"python3 {SCRIPTS_PATH}/generate_synthetic_usage_logs.py "
            f"--rows 50000 "
            f"--mlco2-s3-bucket {S3_BRONZE_BUCKET} --mlco2-s3-prefix mlco2 "
            f"--s3-bucket {S3_BRONZE_BUCKET} --s3-key usage_logs/usage_logs.csv"
        ),
    )

    task_ingest_electricity_maps_api = BashOperator(
        task_id='ingest_electricity_maps_api',
        bash_command=(
            f"python3 {SCRIPTS_PATH}/ingest_electricity_maps.py "
            f"--mode latest "
            f"--zones-s3-bucket {S3_BRONZE_BUCKET} "
            f"--zones-s3-key reference/geo_cloud_to_country_and_zones.csv"
        ),
        env={**os.environ, 'AWS_S3_BUCKET': S3_BRONZE_BUCKET}
    )

    task_bronze_validations_remote = SSHOperator(
        task_id='validate_bronze_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/bronze_validations.py"
        ),
        cmd_timeout=1800,
    )

    # --- FASE 2: SILVER ---
    task_silver_transform_remote = SSHOperator(
        task_id='transform_bronze_to_silver_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE_BUCKET} S3_SILVER_BUCKET={S3_SILVER_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/etl/bronze_to_silver.py"
        ),
        cmd_timeout=3600,
    )

    task_silver_validations_remote = SSHOperator(
        task_id='validate_silver_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE_BUCKET} S3_SILVER_BUCKET={S3_SILVER_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/silver_validations.py"
        ),
        cmd_timeout=3600,
    )

    task_silver_audit_remote = SSHOperator(
        task_id='audit_silver_data_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE_BUCKET} S3_SILVER_BUCKET={S3_SILVER_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/audit.py"
        ),
        cmd_timeout=3600,
    )

    # --- FASE 3: GOLD (Spark remoto) ---
    task_gold_transformation_remote = SSHOperator(
        task_id='build_kimball_gold_layer_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_SILVER_BUCKET={S3_SILVER_BUCKET} S3_GOLD_BUCKET={S3_GOLD_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/etl/silver_to_gold.py"
        ),
        cmd_timeout=3600,
    )

    task_gold_validation_remote = SSHOperator(
        task_id='validate_gold_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_GOLD_BUCKET={S3_GOLD_BUCKET} && "
            f"{SPARK_SUBMIT_BIN} {SPARK_SUBMIT_EXTRA_ARGS} jobs/quality/gold_validations.py --fail"
        ),
        cmd_timeout=3600,
    )

    # --- FLUJO DE EJECUCIÓN ---
    chain(
        task_sources_to_bronze,
        task_build_aws_pricing_reference,
        task_generate_logs,
        task_ingest_electricity_maps_api,
        task_bronze_validations_remote,
        task_silver_transform_remote,
        task_silver_validations_remote,
        task_silver_audit_remote,
        task_gold_transformation_remote,
        task_gold_validation_remote,
    )