"""
Listado de claves de objetos en buckets S3 (solo nombres, sin descargar contenido).
"""

from __future__ import annotations

import os
from functools import lru_cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from utils.project_env import ensure_dotenv_loaded


_NO_CREDS_MSG = (
    "Sin credenciales AWS para S3. En el `.env` del Control Tower definí "
    "`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` y `AWS_DEFAULT_REGION` "
    "(las mismas que Analytics), o ejecutá Streamlit en una máquina con IAM Role "
    "(EC2/ECS) con permisos `s3:ListBucket` en esos buckets."
)


def _friendly_list_error(exc: BaseException) -> str:
    msg = str(exc).strip()
    if "Unable to locate credentials" in msg:
        return _NO_CREDS_MSG
    return msg


def tier_bucket_env_name(tier: str) -> str:
    return {
        "bronze": "DATA_LAKE_BUCKET_BRONZE",
        "silver": "DATA_LAKE_BUCKET_SILVER",
        "gold": "DATA_LAKE_BUCKET_GOLD",
    }[tier]


def tier_prefix_env_name(tier: str) -> str:
    return {
        "bronze": "DATA_LAKE_PREFIX_BRONZE",
        "silver": "DATA_LAKE_PREFIX_SILVER",
        "gold": "DATA_LAKE_PREFIX_GOLD",
    }[tier]


def resolve_bucket_and_prefix(tier: str) -> tuple[str | None, str]:
    b = os.getenv(tier_bucket_env_name(tier), "").strip()
    p = os.getenv(tier_prefix_env_name(tier), "").strip()
    if not b and tier == "gold":
        b = os.getenv("S3_GOLD_BUCKET", "").strip()
    if not b:
        return None, p
    return b, p


@lru_cache(maxsize=1)
def _s3_client():
    return boto3.client("s3")


def list_object_keys(
    tier: str,
    *,
    max_keys: int = 800,
) -> tuple[list[str], str | None]:
    """
    Devuelve (lista de keys ordenadas, mensaje_error o None).
    Si el bucket no está configurado, error amigable sin llamar a AWS.
    """
    ensure_dotenv_loaded()
    bucket, prefix = resolve_bucket_and_prefix(tier)
    if not bucket:
        return [], f"Define {tier_bucket_env_name(tier)} en el entorno."

    keys: list[str] = []
    token: str | None = None
    client = _s3_client()

    try:
        while len(keys) < max_keys:
            kwargs: dict = {
                "Bucket": bucket,
                "MaxKeys": min(1000, max_keys - len(keys)),
            }
            if prefix:
                kwargs["Prefix"] = prefix
            if token:
                kwargs["ContinuationToken"] = token

            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                k = obj.get("Key")
                if k:
                    keys.append(k)
                if len(keys) >= max_keys:
                    break

            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
            if not token:
                break

        keys.sort()
        return keys, None
    except (ClientError, BotoCoreError) as exc:
        return [], _friendly_list_error(exc)
