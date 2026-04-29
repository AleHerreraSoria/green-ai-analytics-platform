"""
Módulo de conexión a S3 y carga de datos desde la capa Gold.
Soporta Delta Lake con fallback a Parquet.
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

# Cargar .env desde múltiples ubicaciones
_env_candidates = [
    _CONTROL_TOWER_ROOT / ".env",
    _ANALYTICS_ROOT / ".env",
    _SRC_DIR / ".env",
    _ANALYTICS_ROOT.parent / ".env",  # raíz del repo
]
for _env_path in _env_candidates:
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
        logger = logging.getLogger(__name__)
        logger.info(f"Cargado .env desde: {_env_path}")

logger = logging.getLogger(__name__)

# Configuración desde environment
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_GOLD_BUCKET = os.getenv("S3_GOLD_BUCKET", "green-ai-pf-gold-a0e96d06")
S3_GOLD_PREFIX = os.getenv("S3_GOLD_PREFIX", "")  # Por defecto vacío


def _get_aws_options() -> dict:
    """Construir opciones de AWS para boto3/deltalake."""
    options = {}
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    
    # Solo pasar credenciales si están definidas y no son placeholders
    if access_key and access_key != "your_access_key_here":
        options["aws_access_key_id"] = access_key
    if secret_key and secret_key != "your_secret_key_here":
        options["aws_secret_access_key"] = secret_key
    if session_token:
        options["aws_session_token"] = session_key
    
    return options


def get_s3_client():
    """Crear cliente S3 de boto3 con credenciales opcionales."""
    import boto3
    
    options = _get_aws_options()
    options["region_name"] = AWS_REGION
    
    return boto3.client('s3', **options)


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


def _load_delta_table(table_uri: str) -> Optional[pd.DataFrame]:
    """
    Intentar cargar una tabla como Delta Lake.
    
    Args:
        table_uri: URI S3 de la tabla (s3://bucket/tabla)
    
    Returns:
        DataFrame si es Delta Table, None si falla
    """
    try:
        from deltalake import DeltaTable
        
        aws_options = _get_aws_options()
        # DeltaTable necesita storage_options
        storage_options = {
            "AWS_REGION": AWS_REGION,
        }
        if aws_options.get("aws_access_key_id"):
            storage_options["AWS_ACCESS_KEY_ID"] = aws_options["aws_access_key_id"]
        if aws_options.get("aws_secret_access_key"):
            storage_options["AWS_SECRET_ACCESS_KEY"] = aws_options["aws_secret_access_key"]
        if aws_options.get("aws_session_token"):
            storage_options["AWS_SESSION_TOKEN"] = aws_options["aws_session_token"]
        
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
        
        logger.info(f"✓ Delta Table leída: {table_uri} -> {len(df)} filas, columnas: {list(df.columns)}")
        return df
    except Exception as e:
        logger.debug(f"Delta Table no disponible para {table_uri}: {e}")
        return None


def _load_parquet_fallback(table_name: str, partitions: Optional[list] = None) -> pd.DataFrame:
    """
    Fallback a Parquet simple, excluyendo _delta_log y checkpoints.
    
    Args:
        table_name: Nombre de la tabla
        partitions: Lista de particiones
    
    Returns:
        DataFrame con los datos parquet válidos
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
        # Intentar sin particiones específicas
        prefix = f"{table_name}/"
        parquet_files = list_parquet_files(prefix)
    
    if not parquet_files:
        logger.warning(f"No se encontraron archivos parquet para {table_name}")
        return pd.DataFrame()
    
    # Filtrar archivos: excluir _delta_log y checkpoints
    valid_files = []
    for f in parquet_files:
        # Excluir _delta_log
        if "/_delta_log/" in f:
            continue
        # Excluir archivos de checkpoint (常见: _delta_log/.../_checkpoint.parquet)
        if "_checkpoint" in f:
            continue
        valid_files.append(f)
    
    logger.info(f"Cargando {len(valid_files)} archivos parquet (fallback) de {table_name}")
    
    dfs = []
    for file_key in valid_files:
        try:
            obj = s3.get_object(Bucket=S3_GOLD_BUCKET, Key=file_key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error cargando {file_key}: {e}")
    
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        logger.info(f"✓ Fallback Parquet: {table_name} -> {len(result)} filas, columnas: {list(result.columns)}")
        return result
    return pd.DataFrame()


def load_table_from_s3(table_name: str, partitions: Optional[list] = None) -> pd.DataFrame:
    """
    Cargar una tabla completa desde S3 Gold.
    Intenta primero Delta Lake, luego fallback a Parquet.
    
    Args:
        table_name: Nombre de la tabla (ej: 'fact_ai_compute_usage')
        partitions: Lista de particiones a cargar (ej: ['year=2024/month=12'])
    
    Returns:
        DataFrame de pandas con los datos
    """
    # Construir URI para Delta
    if partitions:
        partition_path = "/".join(partitions)
        table_uri = f"s3://{S3_GOLD_BUCKET}/{S3_GOLD_PREFIX}{table_name}/{partition_path}"
    else:
        table_uri = f"s3://{S3_GOLD_BUCKET}/{S3_GOLD_PREFIX}{table_name}"
    
    logger.info(f"Intentando leer tabla: {table_name}")
    logger.info(f"  URI S3: {table_uri}")
    
    # Intentar Delta primero
    df = _load_delta_table(table_uri)
    if df is not None:
        return df
    
    # Fallback a Parquet
    logger.info(f"  Fallback a Parquet para: {table_name}")
    return _load_parquet_fallback(table_name, partitions)


def load_dimension(dim_name: str) -> pd.DataFrame:
    """Cargar una tabla de dimensión."""
    return load_table_from_s3(dim_name)


def load_fact_table(fact_name: str, year: Optional[int] = None, 
                    month: Optional[int] = None) -> pd.DataFrame:
    """
    Cargar una tabla de hechos con filtros de partición.
    
    Nota: month sin zero-padding (month=1, no month=01)
    """
    partitions = []
    if year:
        partitions.append(f"year={year}")
    if month:
        partitions.append(f"month={month}")  # Sin :02d
    return load_table_from_s3(fact_name, partitions if partitions else None)

