from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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

# Variables de entorno (inyectadas por Docker/.env)
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SEC = os.getenv('AWS_SECRET_ACCESS_KEY')
EM_TOKEN = os.getenv('ELECTRICITY_MAPS_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# Comando base de Spark con todos los conectores necesarios
# Incluye Delta Lake, Hadoop AWS y AWS SDK Bundle
SPARK_SUBMIT_BASE = (
    "PYTHONPATH=/opt/airflow spark-submit "
    "--master local[*] "
    "--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "--conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' "
    "--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' "
)

# ==========================================================
# FUNCIONES DE APOYO
# ==========================================================

def download_mlco2_from_s3():
    """Descarga diccionarios técnicos (GPUs, Instances)"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    files = ["gpus.csv", "impact.csv", "instances.csv"]
    os.makedirs(MLCO2_LOCAL_DIR, exist_ok=True)
    for f in files:
        dest = os.path.join(MLCO2_LOCAL_DIR, f)
        client.download_file(S3_BRONZE, f"mlco2/{f}", dest)
        print(f"✅ {f} (MLCO2) descargado.")

def sync_external_references():
    """Descarga datos de negocio con rutas específicas en S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    
    external_files = {
        "global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv": "electricity_prices.csv",
        "owid/owid-energy-data.csv": "owid-energy-data.csv",
        "reference/aws_ec2_on_demand_usd_per_hour.csv": "aws_ec2_prices.csv",
        "world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv": "world_bank_tic_exports.csv",
        "world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv": "world_bank_metadata.csv"
    }
    
    os.makedirs(DATA_PATH, exist_ok=True)
    
    for s3_path, local_name in external_files.items():
        dest = os.path.join(DATA_PATH, local_name)
        try:
            client.download_file(S3_BRONZE, s3_path, dest)
            print(f"✅ {local_name} sincronizado.")
        except Exception as e:
            print(f"❌ Error descargando {s3_path}: {e}")
            raise e

# ==========================================================
# DEFINICIÓN DEL DAG
# ==========================================================

with DAG(
    dag_id='ingesta_bronze_silver_e2e_v2',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 4, 21, tz="UTC"),
    catchup=False,
    tags=['green-ai', 'e2e', 'spark', 'delta'],
    default_args={'retries': 1, 'retry_delay': pendulum.duration(minutes=5)}
) as dag:

    # --- FASE 1: BRONZE (Ingesta) ---

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
        bash_command=f"python3 {SCRIPTS_PATH}/generate_synthetic_usage_logs.py --rows 50000 --output {DATA_PATH}/usage_logs.csv"
    )

    task_fetch_api = BashOperator(
        task_id='fetch_carbon_intensity',
        bash_command=(
            f"mkdir -p /opt/airflow/bronze/reference && "
            f"cp {DATA_PATH}/geo_cloud_to_country_and_zones.csv /opt/airflow/bronze/reference/ && "
            f"python3 {SCRIPTS_PATH}/ingest_electricity_maps.py --mode latest"
        ),
        env={
            **os.environ,
            'ELECTRICITY_MAPS_TOKEN': EM_TOKEN,
            'AWS_ACCESS_KEY_ID': AWS_KEY,
            'AWS_SECRET_ACCESS_KEY': AWS_SEC,
            'AWS_S3_BUCKET': S3_BRONZE
        }
    )

    task_upload_s3 = BashOperator(
        task_id='upload_all_to_s3_bronze',
        bash_command=f"python3 {SCRIPTS_PATH}/upload_bronze_to_s3.py --bucket {S3_BRONZE}",
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': AWS_KEY,
            'AWS_SECRET_ACCESS_KEY': AWS_SEC
        }
    )

    # --- FASE 2: SILVER (Refinería Spark) ---

    task_silver_transformation = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command=f"{SPARK_SUBMIT_BASE} {SCRIPTS_PATH}/bronze_to_silver.py",
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': AWS_KEY,
            'AWS_SECRET_ACCESS_KEY': AWS_SEC,
            'S3_BRONZE_BUCKET': S3_BRONZE,
            'S3_SILVER_BUCKET': S3_SILVER
        }
    )

    task_silver_audit = BashOperator(
        task_id='audit_silver_data',
        bash_command=f"{SPARK_SUBMIT_BASE} {SCRIPTS_PATH}/audit.py",
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': AWS_KEY,
            'AWS_SECRET_ACCESS_KEY': AWS_SEC,
            'S3_BRONZE_BUCKET': S3_BRONZE,
            'S3_SILVER_BUCKET': S3_SILVER
        }
    )

    # --- FLUJO DE EJECUCIÓN (ORQUESTACIÓN E2E) ---
    (
        [task_sync_business_data, task_download_mlco2] 
        >> task_generate_logs 
        >> task_fetch_api 
        >> task_upload_s3 
        >> task_silver_transformation 
        >> task_silver_audit
    )