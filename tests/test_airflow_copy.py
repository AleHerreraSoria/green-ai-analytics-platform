"""
TEST: lógica de copy_sources_bucket_to_bronze (sin Airflow)

Validamos:
- que falle si el archivo no existe
- lógica de validación previa al copy
"""

import pytest


def validate_source_exists(head_object_func, bucket, key):
    """
    Replica la lógica del DAG:
    verifica que el archivo exista antes de copiar
    """
    try:
        head_object_func(Bucket=bucket, Key=key)
    except Exception:
        raise ValueError(f"Objeto fuente no encontrado: {key}")


def test_fails_when_file_not_found():
    """
    Simula archivo inexistente
    """

    def fake_head_object(Bucket, Key):
        raise Exception("NoSuchKey")

    with pytest.raises(ValueError):
        validate_source_exists(fake_head_object, "bucket", "file.csv")


def test_pass_when_file_exists():
    """
    Simula archivo existente
    """

    def fake_head_object(Bucket, Key):
        return True

    # no debería fallar
    validate_source_exists(fake_head_object, "bucket", "file.csv")