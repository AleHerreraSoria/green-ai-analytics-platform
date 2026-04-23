from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import pendulum
import os

# ==========================================================
# CONFIGURACIÓN DE RUTAS Y CONSTANTES
# ==========================================================
SCRIPTS_PATH = "/opt/airflow/scripts"
DATA_PATH = "/opt/airflow/data"
MLCO2_LOCAL_DIR = f"{DATA_PATH}/Code_Carbon"

# Buckets de S3
S3_BRONZE = "green-ai-pf-bronze-a0e96d06"
S3_SILVER = "green-ai-pf-silver-a0e96d06"
S3_GOLD = "green-ai-pf-gold-a0e96d06"

SPARK_SSH_CONN_ID = "spark_ssh"
SPARK_REPO_PATH = os.getenv("SPARK_REPO_PATH", "/opt/green-ai-analytics-platform/spark")

# ==========================================================
# FUNCIONES DE APOYO
# ==========================================================

def download_mlco2_from_s3():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    files = ["gpus.csv", "impact.csv", "instances.csv", "2021-10-27yearly_averages.csv"]
    os.makedirs(MLCO2_LOCAL_DIR, exist_ok=True)
    for f in files:
        dest = os.path.join(MLCO2_LOCAL_DIR, f)
        client.download_file(S3_BRONZE, f"mlco2/{f}", dest)
        print(f"✅ {f} (MLCO2) descargado.")

def sync_external_references():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    external_files = {
        "global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv": "electricity_prices.csv",
        "owid/owid-energy-data.csv": "owid-energy-data.csv",
        "reference/aws_ec2_on_demand_usd_per_hour.csv": "aws_ec2_prices.csv",
        "reference/geo_cloud_to_country_and_zones.csv": "geo_cloud_to_country_and_zones.csv",
        "world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv": "world_bank_tic_exports.csv",
        "world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv": "world_bank_metadata.csv"
    }
    os.makedirs(DATA_PATH, exist_ok=True)
    for s3_path, local_name in external_files.items():
        dest = os.path.join(DATA_PATH, local_name)
        client.download_file(S3_BRONZE, s3_path, dest)
        print(f"✅ {local_name} sincronizado.")

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
    task_sync_business_data = PythonOperator(
        task_id='sync_business_references',
        python_callable=sync_external_references
    )

    task_download_mlco2 = PythonOperator(
        task_id='download_mlco2_refs',
        python_callable=download_mlco2_from_s3
    )

    task_generate_logs = BashOperator(
        task_id='generate_usage_logs',
        bash_command=(
            "mkdir -p /opt/airflow/bronze/usage_logs && "
            f"python3 {SCRIPTS_PATH}/generate_synthetic_usage_logs.py "
            "--rows 50000 --output /opt/airflow/bronze/usage_logs/usage_logs.csv"
        )
    )

    task_fetch_api = BashOperator(
        task_id='fetch_carbon_intensity',
        bash_command=(
            f"mkdir -p /opt/airflow/bronze/reference && "
            f"cp {DATA_PATH}/geo_cloud_to_country_and_zones.csv /opt/airflow/bronze/reference/ && "
            f"python3 {SCRIPTS_PATH}/ingest_electricity_maps.py --mode latest"
        ),
        env={**os.environ, 'AWS_S3_BUCKET': S3_BRONZE}
    )

    task_upload_s3 = BashOperator(
        task_id='upload_all_to_s3_bronze',
        bash_command=f"python3 {SCRIPTS_PATH}/upload_bronze_to_s3.py --bucket {S3_BRONZE}"
    )

    task_bronze_validations_remote = SSHOperator(
        task_id='validate_bronze_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE} && "
            "spark-submit jobs/quality/bronze_validations.py"
        ),
        cmd_timeout=1800,
    )

    # --- FASE 2: SILVER ---
    task_silver_transform_remote = SSHOperator(
        task_id='transform_bronze_to_silver_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE} S3_SILVER_BUCKET={S3_SILVER} && "
            "spark-submit jobs/etl/bronze_to_silver.py"
        ),
        cmd_timeout=3600,
    )

    task_silver_validations_remote = SSHOperator(
        task_id='validate_silver_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE} S3_SILVER_BUCKET={S3_SILVER} && "
            "spark-submit jobs/quality/silver_validations.py"
        ),
        cmd_timeout=3600,
    )

    task_silver_audit_remote = SSHOperator(
        task_id='audit_silver_data_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_BRONZE_BUCKET={S3_BRONZE} S3_SILVER_BUCKET={S3_SILVER} && "
            "spark-submit jobs/quality/audit.py"
        ),
        cmd_timeout=3600,
    )

    # --- FASE 3: GOLD (Spark remoto) ---
    task_gold_transformation_remote = SSHOperator(
        task_id='build_kimball_gold_layer_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_SILVER_BUCKET={S3_SILVER} S3_GOLD_BUCKET={S3_GOLD} && "
            "spark-submit jobs/etl/silver_to_gold.py"
        ),
        cmd_timeout=3600,
    )

    task_gold_validation_remote = SSHOperator(
        task_id='validate_gold_quality_remote',
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=(
            f"cd {SPARK_REPO_PATH} && "
            f"export S3_GOLD_BUCKET={S3_GOLD} && "
            "spark-submit jobs/quality/gold_validations.py --fail"
        ),
        cmd_timeout=3600,
    )

    # --- FLUJO DE EJECUCIÓN ---
    (
        [task_sync_business_data, task_download_mlco2] 
        >> task_generate_logs 
        >> task_fetch_api 
        >> task_upload_s3 
        >> task_bronze_validations_remote
        >> task_silver_transform_remote
        >> task_silver_validations_remote
        >> task_silver_audit_remote
        >> task_gold_transformation_remote
        >> task_gold_validation_remote
    )