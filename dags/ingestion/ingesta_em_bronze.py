from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
import pandas as pd
import pendulum
import os  # <--- AGREGÁ ESTA LÍNEA AQUÍ
from datetime import timedelta

# 1. FECHA DE INICIO ESTÁTICA (Soluciona el error de la línea 79)
# Usamos una fecha fija para que el DAG sea consistente en cada parseo.
FECHA_INICIO = pendulum.datetime(2026, 4, 19, tz="UTC")

default_args = {
    'owner': 'Alejandro',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

import os
import glob

def get_zones_from_csv():
    # 1. Buscamos el archivo en TODO el directorio de dags, no importa qué tan profundo esté
    # El patrón /**/ busca en todas las subcarpetas de forma recursiva
    patron_busqueda = "/opt/airflow/dags/**/geo_cloud_to_country_and_zones.csv"
    archivos_encontrados = glob.glob(patron_busqueda, recursive=True)
    
    if archivos_encontrados:
        path_real = archivos_encontrados[0]
        try:
            df = pd.read_csv(path_real)
            zonas = df['electricity_maps_zone'].dropna().unique().tolist()
            print(f"✅ ¡ÉXITO! Archivo encontrado en: {path_real}")
            print(f"📊 Cargadas {len(zonas)} zonas.")
            return zonas
        except Exception as e:
            print(f"❌ Error al leer el archivo encontrado: {e}")
            return ['DE', 'FR']
    else:
        # Si llega acá, es que el archivo NO está dentro del contenedor
        # Vamos a listar lo que SÍ hay para darte la respuesta final
        print("🔍 Listando contenido de /opt/airflow/dags para diagnóstico:")
        for root, dirs, files in os.walk("/opt/airflow/dags"):
            print(f"Directorio: {root} -> Archivos: {files}")
            
        raise FileNotFoundError("❌ El archivo CSV no existe en NINGUNA subcarpeta de /opt/airflow/dags. Verificá tu estructura en Windows.")

def fetch_em_to_s3(zone, **kwargs):
    # Traemos las variables RECIÉN cuando la tarea se ejecuta (Runtime)
    token = Variable.get("em_api_token")
    bucket = Variable.get("s3_bronze")
    
    # Calculamos las fechas relativas al momento de ejecución, no al de parseo
    # Usamos logical_date de Airflow para que sea idempotente
    ejecucion_ts = kwargs.get('logical_date', pendulum.now('UTC'))
    end_date = ejecucion_ts.replace(microsecond=0)
    start_date = end_date.add(days=-1).replace(microsecond=0)
    
    url = "https://api.electricitymaps.com/v4/carbon-intensity/past-range"
    params = {
        'zone': zone,
        'start': start_date.to_iso8601_string(),
        'end': end_date.to_iso8601_string()
    }
    headers = {'auth-token': token}

    response = requests.get(url, params=params, headers=headers)
    
    if response.status_code == 400:
        print(f"❌ Zona '{zone}' no disponible.")
        return

    response.raise_for_status()
    data = response.json()

    s3_key = (
        f"electricity_maps/zone={zone}/"
        f"year={end_date.year}/month={end_date.month:02d}/day={end_date.day:02d}/data.json"
    )

    s3 = S3Hook(aws_conn_id='aws_default')
    s3.load_string(
        string_data=json.dumps(data),
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )
    print(f"✅ Éxito: {zone}")

# 4. DEFINICIÓN DEL DAG (Limpio de valores variables)
with DAG(
    dag_id='ingesta_em_bronze_SAFE_V4',
    default_args=default_args,
    description='Ingesta multizona optimizada para recursos limitados',
    schedule='@daily',
    start_date=FECHA_INICIO,  # <--- VALOR ESTÁTICO (Fix Line 79)
    catchup=False,
    max_active_tasks=3,
    max_active_runs=1,
    tags=['green-ai', 'safe-mode']
) as dag:

    zonas = get_zones_from_csv()
    
    for zona in zonas:
        PythonOperator(
            task_id=f"fetch_{zona.replace('-', '_')}",
            python_callable=fetch_em_to_s3,
            op_kwargs={'zone': zona}
        )