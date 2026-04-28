"""
Valida que las reglas de negocio del pipeline, filtrado de datos inválidos, dominios correctos 
y generación de columnas derivadas funcionen correctamente sobre los datos.

"""

import pandas as pd


def test_usage_logs_business_rules_no_spark():

    # -----------------------------
    # 1. Dataset de prueba
    # -----------------------------
    data = [
        ("2024-01-01 10:00:00", 10.0, 2.0, 0.5, "Training", "Success"),  # válido

        (None, 10.0, 2.0, 0.5, "Training", "Success"),                  # timestamp null
        ("2024-01-01 10:00:00", -5.0, 2.0, 0.5, "Training", "Success"), # energía inválida
        ("2024-01-01 10:00:00", 10.0, -1.0, 0.5, "Training", "Success"),# duración inválida
        ("2024-01-01 10:00:00", 10.0, 2.0, 1.5, "Training", "Success"), # gpu inválido
        ("2024-01-01 10:00:00", 10.0, 2.0, 0.5, "Invalid", "Success"),  # job_type inválido
    ]

    df = pd.DataFrame(data, columns=[
        "timestamp",
        "energy_consumed_kwh",
        "duration_hours",
        "gpu_utilization",
        "job_type",
        "execution_status"
    ])

    # -----------------------------
    # 2. Transformaciones
    # -----------------------------
    # Convertir timestamp (equivalente a cast en Spark)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Columnas derivadas
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    # Dominios válidos
    VALID_JOB_TYPES = {"Training", "Inference", "Fine-tuning"}
    VALID_STATUSES = {"Success", "Failed"}

    # -----------------------------
    # 3. Aplicar reglas de negocio
    # -----------------------------
    df_clean = df[
        (df["timestamp"].notna()) &
        (df["energy_consumed_kwh"] > 0) &
        (df["duration_hours"] > 0) &
        (df["gpu_utilization"] >= 0) &
        (df["gpu_utilization"] <= 1) &
        (df["job_type"].isin(VALID_JOB_TYPES)) &
        (df["execution_status"].isin(VALID_STATUSES))
    ]

    # -----------------------------
    # 4. Validaciones (asserts)
    # -----------------------------

    # Solo debe quedar 1 fila válida
    assert len(df_clean) == 1

    # Validaciones de calidad
    assert (df_clean["energy_consumed_kwh"] <= 0).sum() == 0
    assert (df_clean["duration_hours"] <= 0).sum() == 0
    assert (df_clean["gpu_utilization"] > 1).sum() == 0

    # Validación de nulls
    assert df_clean["timestamp"].isna().sum() == 0

    # Validación de dominio
    assert df_clean["job_type"].isin(VALID_JOB_TYPES).all()

    # Validación de columnas derivadas
    assert "year" in df_clean.columns
    assert "month" in df_clean.columns