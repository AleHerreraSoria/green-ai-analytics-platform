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

# Capturamos las variables del .env (inyectadas por Docker)
# Si alguna falta, el operador fallará preventivamente
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SEC = os.getenv('AWS_SECRET_ACCESS_KEY')
EM_TOKEN = os.getenv('ELECTRICITY_MAPS_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

def download_mlco2_from_s3():
    """Descarga de diccionarios MLCO2 usando el cliente directo de S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    hook = S3Hook(aws_conn_id='aws_default')
    client = hook.get_conn()
    files = ["gpus.csv", "impact.csv", "instances.csv"]
    
    os.makedirs(MLCO2_LOCAL_DIR, exist_ok=True)
    
    for f in files:
        dest = os.path.join(MLCO2_LOCAL_DIR, f)
        print(f"📦 Descargando {f} desde S3...")
        client.download_file(S3_BUCKET, f"mlco2/{f}", dest)
        print(f"✅ {f} guardado en {dest}")

with DAG(
    dag_id='ingesta_bronze_total_v2',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 4, 21, tz="UTC"),
    catchup=False,
    tags=['green-ai', 'bronze', 'production'],
    default_args={'retries': 1, 'retry_delay': pendulum.duration(minutes=5)}
) as dag:

    # 1. Preparar Referencias MLCO2 (Desde S3)
    task_download_refs = PythonOperator(
        task_id='download_mlco2_refs',
        python_callable=download_mlco2_from_s3
    )

    # 2. Generar Logs Sintéticos (Script de Eduardo)
    task_generate_logs = BashOperator(
        task_id='generate_usage_logs',
        bash_command=f"python3 {SCRIPTS_PATH}/generate_synthetic_usage_logs.py --rows 50000 --output {DATA_PATH}/usage_logs.csv"
    )

    # 3. Ingesta de Electricity Maps (API)
    # Usa la ruta de geo_cloud que encontramos con 'find'
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

    # 4. Subir todos los resultados locales a S3 Bronze
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
    task_download_refs >> task_generate_logs >> task_fetch_api >> task_upload_s3