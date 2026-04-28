"""
Módulo de conexión a S3 y carga de datos desde la capa Gold.
"""
import io
import os
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

_SRC_DIR = Path(__file__).resolve().parent
_ANALYTICS_ROOT = _SRC_DIR.parent
_CONTROL_TOWER_ROOT = _ANALYTICS_ROOT.parent

for _env_path in (_CONTROL_TOWER_ROOT / ".env", _ANALYTICS_ROOT / ".env"):
    load_dotenv(_env_path, override=False)

logger = logging.getLogger(__name__)

# Configuración desde environment
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_GOLD_BUCKET = os.getenv("S3_GOLD_BUCKET", "green-ai-pf-gold-a0e96d06")


def get_s3_client():
    """Crear cliente S3 de boto3."""
    import boto3
    return boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )


def list_parquet_files(prefix: str) -> list:
    """Listar archivos parquet en un prefijo del bucket S3 (paginado)."""
    s3 = get_s3_client()
    keys: list[str] = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_GOLD_BUCKET, Prefix=prefix):
            for obj in page.get("Contents") or []:
                k = obj["Key"]
                if k.endswith(".parquet"):
                    keys.append(k)
        return keys
    except Exception as e:
        logger.warning(f"No se pudieron listar archivos en {prefix}: {e}")
        return []


def load_table_from_s3(table_name: str, partitions: Optional[list] = None) -> pd.DataFrame:
    """
    Cargar una tabla completa desde S3 Gold.
    
    Args:
        table_name: Nombre de la tabla (ej: 'fact_ai_compute_usage')
        partitions: Lista de particiones a cargar (ej: ['year=2024/month=12'])
    
    Returns:
        DataFrame de pandas con los datos
    """
    s3 = get_s3_client()
    
    # Construir prefijo
    if partitions:
        partition_path = "/".join(partitions)
        prefix = f"{table_name}/{partition_path}/"
    else:
        prefix = f"{table_name}/"
    
    # Listar archivos parquet
    parquet_files = list_parquet_files(prefix)
    
    if not parquet_files:
        # Intentar cargar sin particiones específicas
        prefix = f"{table_name}/"
        parquet_files = list_parquet_files(prefix)
    
    if not parquet_files:
        logger.warning(f"No se encontraron archivos parquet para {table_name}")
        return pd.DataFrame()
    
    logger.info(f"Cargando {len(parquet_files)} archivos de {table_name}")
    
    # Cargar cada archivo y concatenar
    dfs = []
    for file_key in parquet_files:
        try:
            obj = s3.get_object(Bucket=S3_GOLD_BUCKET, Key=file_key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error cargando {file_key}: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def load_dimension(dim_name: str) -> pd.DataFrame:
    """Cargar una tabla de dimensión."""
    return load_table_from_s3(dim_name)


def load_fact_table(fact_name: str, year: Optional[int] = None, 
                    month: Optional[int] = None) -> pd.DataFrame:
    """Cargar una tabla de hechos con filtros de partición."""
    partitions = []
    if year:
        partitions.append(f"year={year}")
    if month:
        partitions.append(f"month={month:02d}")
    return load_table_from_s3(fact_name, partitions if partitions else None)

