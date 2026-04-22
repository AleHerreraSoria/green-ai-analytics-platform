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
S3_BUCKET = "green-ai-pf-bronze-a0e96d06"

# Variables de entorno seguras (inyectadas por .env y Docker)
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SEC = os.getenv('AWS_SECRET_ACCESS_KEY')
EM_TOKEN = os.getenv('ELECTRICITY_MAPS_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

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
        client.download_file(S3_BUCKET, f"mlco2/{f}", dest)
        print(f"✅ {f} (MLCO2) descargado.")

def sync_external_references():
    """Descarga datos de negocio con rutas específicas en S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    
    # Mapeo de RUTA_EN_S3: NOMBRE_LOCAL
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
        print(f"📦 Bajando desde S3: {s3_path}...")
        try:
            client.download_file(S3_BUCKET, s3_path, dest)
            print(f"✅ Guardado localmente como: {local_name}")
        except Exception as e:
            print(f"❌ Error descargando {s3_path}: {e}")
            raise e

# ==========================================================
# DEFINICIÓN DEL DAG
# ==========================================================

with DAG(
    dag_id='ingesta_bronze_total_v2',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 4, 21, tz="UTC"),
    catchup=False,
    tags=['green-ai', 'bronze', 'production'],
    default_args={'retries': 1, 'retry_delay': pendulum.duration(minutes=5)}
) as dag:

    # 1. Sincronizar datos externos de Negocio (Precios, PIB, etc.)
    task_sync_business_data = PythonOperator(
        task_id='sync_business_references',
        python_callable=sync_external_references
    )

    # 2. Preparar Referencias Técnicas (MLCO2)
    task_download_mlco2 = PythonOperator(
        task_id='download_mlco2_refs',
        python_callable=download_mlco2_from_s3
    )

    # 3. Generar Logs Sintéticos (50k registros)
    task_generate_logs = BashOperator(
        task_id='generate_usage_logs',
        bash_command=f"python3 {SCRIPTS_PATH}/generate_synthetic_usage_logs.py --rows 50000 --output {DATA_PATH}/usage_logs.csv"
    )

    # 4. Ingesta de Electricity Maps (API)
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
            'AWS_DEFAULT_REGION': AWS_REGION,
            'AWS_S3_BUCKET': S3_BUCKET
        }
    )

    # 5. Subir TODO a S3 Bronze (Incluye los nuevos archivos de negocio)
    task_upload_s3 = BashOperator(
        task_id='upload_all_to_s3_bronze',
        bash_command=f"python3 {SCRIPTS_PATH}/upload_bronze_to_s3.py --bucket {S3_BUCKET}",
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': AWS_KEY,
            'AWS_SECRET_ACCESS_KEY': AWS_SEC,
            'AWS_DEFAULT_REGION': AWS_REGION
        }
    )

    # Flujo de ejecución
    task_sync_business_data >> task_download_mlco2 >> task_generate_logs >> task_fetch_api >> task_upload_s3