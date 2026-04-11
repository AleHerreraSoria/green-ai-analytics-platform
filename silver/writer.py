"""
writer.py
==========
Módulo centralizado de escritura en la capa Silver del proyecto Green AI.

Responsabilidades:
  - Estandarizar la escritura en formato Parquet con compresión Snappy.
  - Aplicar la estrategia de particionado lógico por tipo de dataset.
  - Detectar y validar que el DataFrame no esté vacío antes de escribir.
  - Devolver metadata de escritura (conteo de filas, path) para el módulo
    de auditoría (audit.py).

No contiene lógica de transformación. Se importa desde bronze_to_silver.py
y puede ser reutilizado por cualquier job Silver futuro.
"""

import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ── Logging ────────────────────────────────────────────────────────────────
# writer.py es importado por otros jobs — hereda la config básica del
# basicConfig llamado en bronze_to_silver.py/audit.py/validations.py.
# Si se instancia solo (ej. en tests), configura su propio handler.
logger = logging.getLogger("green-ai.writer")


# ---------------------------------------------------------------------------
# Estrategias de particionado por tipo de dataset
# ---------------------------------------------------------------------------
# Determina las columnas de partición Hive para cada dataset Silver.
# Se define como constante para que sea auditable y documentable.

PARTITION_STRATEGIES: dict[str, list[str]] = {
    # Electricity Maps: zona eléctrica + ventana temporal
    "electricity_maps/carbon_intensity/latest":  ["zone", "year", "month"],
    "electricity_maps/carbon_intensity/past":    ["zone", "year", "month"],
    "electricity_maps/carbon_intensity/history": ["zone", "year", "month"],
    "electricity_maps/electricity_mix/latest":   ["zone", "year", "month"],
    "electricity_maps/zones/catalog":            ["country_code"],

    # Catálogos estáticos: sin partición (tabla pequeña de referencia)
    "global_petrol_prices":  [],
    "reference/ec2_pricing": ["cloud_provider"],
    "mlco2/yearly_averages": ["year"],

    # Macrodatos históricos: año es la dimensión natural
    "owid":                  ["year"],
    "world_bank/ict_exports": ["year"],

    # Logs: año + mes + región (alta cardinalidad → 3 niveles)
    "usage_logs":            ["year", "month", "region"],
}


# ---------------------------------------------------------------------------
# Resultado de escritura
# ---------------------------------------------------------------------------

@dataclass
class WriteResult:
    dataset_key: str
    silver_path: str
    rows_written: int
    partition_by: list[str]
    status: str = "OK"
    error: str | None = None

    def __str__(self) -> str:
        parts = f"  particiones: {self.partition_by}" if self.partition_by else "  sin partición"
        return (
            f"[{self.status}] {self.dataset_key}\n"
            f"    path:    {self.silver_path}\n"
            f"    filas:   {self.rows_written:,}\n"
            f"{parts}"
        )


# ---------------------------------------------------------------------------
# Writer principal
# ---------------------------------------------------------------------------

class SilverWriter:
    """
    Clase responsable de estandarizar la escritura de DataFrames en Silver.

    Parámetros
    ----------
    silver_bucket : str
        Nombre del bucket S3 de destino (ej. 'green-ai-pf-silver-a0e96d06').
    mode : str
        Modo de escritura Spark ('overwrite' por defecto; también 'append').
    """

    def __init__(self, silver_bucket: str, mode: str = "overwrite"):
        self.silver_bucket = silver_bucket
        self.mode = mode

    def _build_path(self, dataset_key: str) -> str:
        return f"s3://{self.silver_bucket}/{dataset_key}"

    def write(self, df: DataFrame, dataset_key: str) -> WriteResult:
        """
        Escribe el DataFrame en la ruta Silver correspondiente al dataset_key.

        Parámetros
        ----------
        df : DataFrame
            DataFrame transformado listo para Silver.
        dataset_key : str
            Clave del dataset (debe existir en PARTITION_STRATEGIES o se
            aplica escritura sin partición con advertencia).

        Retorna
        -------
        WriteResult con la metadata de la operación.
        """
        silver_path = self._build_path(dataset_key)
        partition_by = PARTITION_STRATEGIES.get(dataset_key)

        if partition_by is None:
            logger.warning(
                "dataset_key '%s' sin estrategia de particionado. Se escribe sin partición.",
                dataset_key,
            )
            partition_by = []

        # Verificar que el DataFrame no esté vacío
        if df.isEmpty():
            logger.warning("DataFrame vacío para '%s'; escritura omitida.", dataset_key)
            return WriteResult(
                dataset_key=dataset_key,
                silver_path=silver_path,
                rows_written=0,
                partition_by=partition_by,
                status="WARN",
                error="DataFrame vacío; escritura omitida.",
            )

        try:
            writer = (
                df.write
                .mode(self.mode)
                .format("parquet")
                .option("compression", "snappy")
            )

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(silver_path)

            rows_written = df.count()

            return WriteResult(
                dataset_key=dataset_key,
                silver_path=silver_path,
                rows_written=rows_written,
                partition_by=partition_by,
                status="OK",
            )

        except Exception as exc:
            return WriteResult(
                dataset_key=dataset_key,
                silver_path=silver_path,
                rows_written=0,
                partition_by=partition_by,
                status="ERROR",
                error=str(exc),
            )

    def write_batch(
        self,
        datasets: dict[str, DataFrame],
    ) -> list[WriteResult]:
        """
        Escribe múltiples DataFrames en lote. Ideal para el job principal.

        Parámetros
        ----------
        datasets : dict[str, DataFrame]
            Mapa de dataset_key → DataFrame transformado.

        Retorna
        -------
        Lista de WriteResult para consumo de audit.py.
        """
        results = []
        for key, df in datasets.items():
            print(f"\n[writer] Escribiendo '{key}' en Silver...")
            result = self.write(df, key)
            print(result)
            results.append(result)
        return results


# ---------------------------------------------------------------------------
# Función de conveniencia (sin instanciar la clase)
# ---------------------------------------------------------------------------

def write_to_silver(
    df: DataFrame,
    dataset_key: str,
    silver_bucket: str | None = None,
    mode: str = "overwrite",
) -> WriteResult:
    """
    Wrapper funcional sobre SilverWriter para uso directo en bronze_to_silver.py.
    """
    bucket = silver_bucket or os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")
    writer = SilverWriter(silver_bucket=bucket, mode=mode)
    return writer.write(df, dataset_key)


# ---------------------------------------------------------------------------
# CLI de diagnóstico
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    # En modo CLI, lista las estrategias de particionado definidas
    print("\n Estrategias de particionado Silver registradas:\n")
    print(f"  {'Dataset Key':<50} {'Partition Columns'}")
    print(f"  {'-'*50} {'-'*30}")
    for key, parts in PARTITION_STRATEGIES.items():
        parts_str = ", ".join(parts) if parts else "(sin partición)"
        print(f"  {key:<50} {parts_str}")
    print()
