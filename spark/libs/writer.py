"""
writer.py
==========
Módulo centralizado de escritura en la capa Silver del proyecto Green AI.

Responsabilidades:
  - Estandarizar la escritura en formato Delta con compresión Snappy.
  - Aplicar la estrategia de particionado lógico por tipo de dataset.
  - Detectar y validar que el DataFrame no esté vacío antes de escribir.
  - Implementar escritura idempotente mediante operaciones MERGE nativas de Delta.
  - Devolver metadata de escritura (conteo de filas, path) para el módulo
    de auditoría (audit.py).

Estrategia de escritura:
  - Primera ejecución (tabla no existe): bootstrap con .write.format("delta").
  - Ejecuciones subsiguientes: MERGE (Upsert) sobre la llave natural del dataset,
    garantizando idempotencia completa sin duplicados ni pérdida de datos.

No contiene lógica de transformación. Se importa desde bronze_to_silver.py
y puede ser reutilizado por cualquier job Silver futuro.
"""

from __future__ import annotations
import logging
import os
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ── Logging ────────────────────────────────────────────────────────────────
# writer.py es importado por otros jobs — hereda la config básica del
# basicConfig llamado en bronze_to_silver.py/audit.py/validations.py.
# Si se instancia solo (ej. en tests), configura su propio handler.
logger = logging.getLogger("green-ai.writer")


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


# ---------------------------------------------------------------------------
# Estrategias de particionado por tipo de dataset
# ---------------------------------------------------------------------------
# Determina las columnas de partición Hive para cada dataset Silver.
# Se define como constante para que sea auditable y documentable.

PARTITION_STRATEGIES: dict[str, list[str]] = {
    # 1. Electricity Maps (Streaming / Alto Volumen)
    "electricity_maps/carbon_intensity/latest":  ["year", "month"],
    "electricity_maps/carbon_intensity/past":    ["year", "month"],
    "electricity_maps/carbon_intensity/history": ["year", "month"],
    "electricity_maps/electricity_mix/latest":   ["year", "month"],

    # Catálogos estáticos: Nunca se particionan. Son diminutos (KBs/MBs).
    "reference/zones_dimension": [],
    "global_petrol_prices":  [],
    "reference/ec2_pricing": [],
    "reference/geo_cloud_mapping": [],
    "mlco2/yearly_averages": [],
    "mlco2/instances": [],
    "mlco2/impact": [],
    "mlco2/gpus": [],
    "reference/world_bank_metadata": [],

    # 2. Macrodatos históricos (Bajo Volumen)
    "owid":                  [],
    "world_bank/ict_exports": [],

    # 3. Logs de uso (Volumen Medio/Alto)
    "usage_logs":            ["year", "month"],
}


# ---------------------------------------------------------------------------
# Llaves de MERGE por dataset (llave natural inferida del esquema Silver)
# ---------------------------------------------------------------------------
# Estas columnas forman la condición ON del MERGE. Deben identificar
# unívocamente cada fila en la tabla de destino.
# Criterio: campos nullable=False del esquema + granularidad temporal cuando
# el dataset tiene partición (series de tiempo).

MERGE_KEYS: dict[str, list[str]] = {
    # Series temporales — llave compuesta zona + timestamp de evento
    "electricity_maps/carbon_intensity/latest":  ["zone", "datetime"],
    "electricity_maps/carbon_intensity/past":    ["zone", "datetime"],
    "electricity_maps/carbon_intensity/history": ["zone", "datetime"],
    "electricity_maps/electricity_mix/latest":   ["zone", "datetime"],

    # Catálogos de referencia — llave natural simple o compuesta
    "reference/zones_dimension":    ["zone_key"],
    "global_petrol_prices":         ["country"],
    "reference/ec2_pricing":        [
        "cloud_provider", "cloud_region", "instance_type",
        "operating_system", "pricing_model",
    ],
    "reference/geo_cloud_mapping":  ["cloud_provider", "cloud_region"],
    "mlco2/yearly_averages":        ["zone_key", "year"],
    "mlco2/instances":              ["id"],
    "mlco2/impact":                 ["region"],
    "mlco2/gpus":                   ["gpu_model"],

    # Macrodatos históricos — llave compuesta entidad + año
    "owid":                         ["country", "year"],
    "world_bank/ict_exports":       ["country_code", "year"],
    "reference/world_bank_metadata": ["country_code"],

    # Logs de sesión — session_id es UUID único por sesión completa
    "usage_logs":                   ["session_id"],
}


# ---------------------------------------------------------------------------
# Datasets de full-reload por partición
# ---------------------------------------------------------------------------
# Estos datasets leen SIEMPRE el conjunto completo de datos desde Bronze
# (no son incrementales). Para ellos, la estrategia correcta de escritura
# es replaceWhere: reemplaza atómicamente las particiones afectadas con
# el source limpio y deduplicado, en lugar de MERGE (que nunca hace DELETE
# y deja huérfanos los registros contaminados en el target).
#
# Criterio de inclusión: el job de transformación lee recursivamente TODOS
# los archivos del path Bronze en cada ejecución (recursiveFileLookup=true)
# y el DataFrame resultante representa el estado completo y definitivo de
# cada partición (year, month) presente en el source.
REPLACE_PARTITION_DATASETS: set[str] = {
    # El endpoint /history entrega snapshots actualizados acumulativos.
    # Cada run re-lee todos los JSON de Bronze y produce el estado óptimo
    # (mayor updatedAt) para cada (zone, datetime). Con MERGE, las filas
    # huérfanas de runs anteriores persisten indefinidamente. Con
    # replaceWhere, cada run sobreescribe limpiamente las particiones.
    "electricity_maps/carbon_intensity/history",
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
# Helper central de escritura idempotente (MERGE / bootstrap)
# ---------------------------------------------------------------------------

def _delta_replace_partitions(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    partition_by: list[str],
) -> None:
    """
    Escritura idempotente para datasets de full-reload por partición.

    Estrategia: Delta replaceWhere
    ──────────────────────────────
    En lugar del patrón MERGE (UPDATE + INSERT, nunca DELETE), esta función
    usa el modo overwrite con replaceWhere de Delta Lake, que:

      1. Identifica las particiones (year, month, …) presentes en el source.
      2. Elimina atómicamente TODO el contenido de esas particiones del target.
      3. Escribe el source limpio (ya deduplicado upstream) en su lugar.

    El resultado es siempre igual al source: idempotente por construcción.
    Ninguna fila huérfana de ejecuciones anteriores puede sobrevivir.

    Cuándo usar replaceWhere vs MERGE
    ──────────────────────────────────
    MERGE es correcto para datasets incrementales donde el source es un
    "delta" (solo las novedades). replaceWhere es correcto para datasets
    donde el source es el estado completo de cada partición (full-reload).

    Parámetros
    ----------
    spark        : SparkSession activa.
    df           : DataFrame deduplicado listo para escribir.
    path         : Ruta S3 de la tabla Delta de destino.
    merge_keys   : No se usan aquí; se mantiene la firma por consistencia
                   con _delta_merge() para que el caller sea intercambiable.
    partition_by : Columnas de partición Hive. Requerido: debe ser no-vacío
                   para que replaceWhere pueda acotar el reemplazo.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        # ── Bootstrap: primera escritura, tabla nueva ──────────────────────────────
        logger.info("[REPLACE] Bootstrap (tabla nueva): %s", path)
        writer = df.write.format("delta").option("compression", "snappy")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return

    # ── Calcular el predicado replaceWhere a partir de las particiones del source ──
    # Recolectamos los valores distintos de cada columna de partición que
    # están presentes en el source. Delta usará este predicado para eliminar
    # solo las particiones afectadas, sin tocar el resto de la tabla.
    replace_clauses: list[str] = []
    source_cols = set(df.columns)
    for part_col in partition_by:
        if part_col not in source_cols:
            logger.warning(
                "[REPLACE] Columna de partición '%s' no encontrada en el source; "
                "omitida del predicado replaceWhere.",
                part_col,
            )
            continue
        distinct_vals = [
            row[part_col]
            for row in df.select(part_col).distinct().collect()
            if row[part_col] is not None
        ]
        if not distinct_vals:
            continue
        if isinstance(distinct_vals[0], str):
            in_list = ", ".join(f"'{v}'" for v in distinct_vals)
        else:
            in_list = ", ".join(str(v) for v in distinct_vals)
        replace_clauses.append(f"`{part_col}` IN ({in_list})")

    if not replace_clauses:
        # Sin predicado válido: fall back a overwrite total (raro, defensivo)
        logger.warning(
            "[REPLACE] No se pudo construir predicado replaceWhere para '%s'. "
            "Se usará overwrite completo.",
            path,
        )
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("compression", "snappy")
            .save(path)
        )
        return

    replace_where = " AND ".join(replace_clauses)
    logger.info(
        "[REPLACE] replaceWhere sobre '%s': %s", path, replace_where
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .option("compression", "snappy")
        .save(path)
    )


def _delta_merge(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    partition_by: list[str] | None = None,
) -> None:
    """
    Escritura idempotente sobre una tabla Delta usando MERGE nativo.

    Flujo:
      1. Si la tabla Delta **no existe** (primera ejecución / bootstrap):
         escribe directamente con .write.format("delta"), aplicando el
         particionado configurado. Esto es equivalente al comportamiento
         anterior pero con semántica de creación inicial.

      2. Si la tabla **ya existe**:
         ejecuta un MERGE ON <merge_keys> que:
           - WHEN MATCHED         → UPDATE SET * (actualiza todos los campos)
           - WHEN NOT MATCHED     → INSERT *     (inserta filas nuevas)
         Garantiza idempotencia total: re-ejecuciones sobre los mismos datos
         no generan duplicados ni pérdida de información.

    Parámetros
    ----------
    spark       : SparkSession activa (requerida para DeltaTable.forPath).
    df          : DataFrame transformado listo para escribir.
    path        : Ruta S3 de la tabla Delta de destino.
    merge_keys  : Columnas que forman la condición ON del MERGE.
                  Deben identificar unívocamente cada fila.
    partition_by: Columnas de partición Hive (sólo se aplican en bootstrap).
                  En MERGE posteriores Delta gestiona las particiones
                  automáticamente conforme a la definición original de la tabla.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        # ── Bootstrap: primera escritura de la tabla ─────────────────────────
        logger.info("[MERGE] Bootstrap (tabla nueva): %s", path)
        writer = df.write.format("delta").option("compression", "snappy")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return

    # ── MERGE: escritura idempotente sobre tabla existente ───────────────────
    logger.info(
        "[MERGE] Ejecutando MERGE sobre '%s' con keys=%s partition_by=%s",
        path, merge_keys, partition_by or [],
    )

    # ── Construcción de la condición MERGE en dos niveles ─────────────────
    #
    # Nivel 1 — Partition Pruning (sólo si la tabla está particionada):
    #   Para cada columna de partición, se recolectan los valores distintos
    #   presentes en el lote SOURCE y se inyectan como predicado IN sobre
    #   la columna TARGET correspondiente.
    #
    #   Ejemplo para partition_by=["year","month"] con un lote de enero-2025:
    #     target.`year` IN (2025) AND target.`month` IN (1)
    #
    #   Delta Lake interpreta estos predicados sobre columnas de partición
    #   ANTES de abrir ningún archivo Parquet, descartando todas las
    #   particiones que no coincidan (partition pruning a nivel de plan físico).
    #   Esto convierte un full-table scan en una lectura dirigida.
    #
    # Nivel 2 — Row-level join:
    #   Las merge_keys originales identifican la fila exacta dentro de las
    #   particiones ya podadas. Sin el Nivel 1 estas llaves requerirían
    #   escanear TODAS las particiones.
    # ────────────────────────────────────────────────────────────────────────

    # ── Deduplicar source por merge_keys (requisito de Delta MERGE) ──────────
    # Delta exige que el source DataFrame tenga como máximo UNA fila por cada
    # valor de la clave de merge. Si el source contiene duplicados por la clave
    # (ej. el API de ElectricityMaps puede devolver el mismo (zone, datetime)
    # más de una vez en un lote), Delta lanza:
    #   multipleSourceRowMatchingTargetRowInMergeException
    # dropDuplicates(merge_keys) resuelve esto de forma determinista:
    # conserva la primera fila por clave según el orden del DataFrame.
    df = df.dropDuplicates(merge_keys)

    # Nivel 1: predicados de pruning derivados de las columnas de partición.
    # Se excluyen columnas de partición que ya son merge_keys (evita redundancia).
    pruning_clauses: list[str] = []
    effective_partition_cols = [
        p for p in (partition_by or []) if p not in merge_keys
    ]
    if effective_partition_cols:
        source_cols = set(df.columns)
        for part_col in effective_partition_cols:
            if part_col not in source_cols:
                # Columna de partición ausente en el source (ej. derivada
                # externamente): se omite este predicado con aviso.
                logger.warning(
                    "[MERGE] Columna de partición '%s' no encontrada en el "
                    "source DataFrame; pruning omitido para esta columna.",
                    part_col,
                )
                continue
            # Recolectar valores distintos del lote actual
            distinct_vals = [
                row[part_col]
                for row in df.select(part_col).distinct().collect()
                if row[part_col] is not None
            ]
            if not distinct_vals:
                continue
            # Formatear según tipo: strings van con comillas, numéricos sin ellas
            if isinstance(distinct_vals[0], str):
                in_list = ", ".join(f"'{v}'" for v in distinct_vals)
            else:
                in_list = ", ".join(str(v) for v in distinct_vals)
            pruning_clauses.append(f"target.`{part_col}` IN ({in_list})")

    # Nivel 2: equi-join sobre las llaves primarias / naturales del dataset
    key_clauses = [
        f"target.`{k}` = source.`{k}`" for k in merge_keys
    ]

    # Condición final: pruning (si aplica) AND equi-join de llave
    all_clauses = pruning_clauses + key_clauses
    merge_condition = " AND ".join(all_clauses)

    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
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
    spark : SparkSession
        Sesión Spark activa, requerida para las operaciones MERGE de Delta.
    """

    def __init__(self, silver_bucket: str, spark: SparkSession):
        self.silver_bucket = silver_bucket
        self.spark = spark

    def _build_path(self, dataset_key: str) -> str:
        return f"s3a://{self.silver_bucket}/{dataset_key}"

    def write(self, df: DataFrame, dataset_key: str) -> WriteResult:
        """
        Escribe el DataFrame en la ruta Silver correspondiente al dataset_key
        usando una operación MERGE idempotente de Delta Lake.

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

        merge_keys = MERGE_KEYS.get(dataset_key)
        if merge_keys is None:
            logger.warning(
                "dataset_key '%s' sin MERGE_KEYS definidas. Se usará overwrite como fallback.",
                dataset_key,
            )

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
            if partition_by:
                # ── SELF-HEALING (Memoria local PySpark → S3) ──────────────
                # Repartition colapsa los datos en memoria por llave de partición
                # antes de escribir. Evita que el worker local crashee (OOM / EOFException)
                # al intentar abrir cientos de conexiones S3A y archivos simultáneamente.
                df = df.repartition(1, *partition_by)

            if dataset_key in REPLACE_PARTITION_DATASETS:
                # ── Estrategia: replaceWhere (full-reload por partición) ───────
                # Para datasets que re-leen siempre el conjunto completo de
                # datos desde Bronze. replaceWhere elimina atómicamente las
                # particiones afectadas y las reescribe limpias, garantizando
                # que ningún duplicado histórico sobreviva entre ejecuciones.
                if not partition_by:
                    logger.error(
                        "[REPLACE] dataset_key '%s' está en REPLACE_PARTITION_DATASETS "
                        "pero no tiene partition_by. Se usará MERGE como fallback.",
                        dataset_key,
                    )
                    _delta_merge(
                        spark=self.spark,
                        df=df,
                        path=silver_path,
                        merge_keys=merge_keys,
                        partition_by=None,
                    )
                else:
                    _delta_replace_partitions(
                        spark=self.spark,
                        df=df,
                        path=silver_path,
                        merge_keys=merge_keys or [],
                        partition_by=partition_by,
                    )
            elif merge_keys:
                # ── Escritura idempotente con MERGE nativo de Delta ────────
                _delta_merge(
                    spark=self.spark,
                    df=df,
                    path=silver_path,
                    merge_keys=merge_keys,
                    partition_by=partition_by if partition_by else None,
                )
            else:
                # ── Fallback: overwrite (dataset sin MERGE_KEYS definidas) ─
                writer = (
                    df.write
                    .format("delta")
                    .mode("overwrite")
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
    spark: SparkSession | None = None,
    silver_bucket: str | None = None,
) -> WriteResult:
    """
    Wrapper funcional sobre SilverWriter para uso directo en bronze_to_silver.py.

    Parámetros
    ----------
    df          : DataFrame transformado.
    dataset_key : Clave de destino Silver (debe existir en MERGE_KEYS).
    spark       : SparkSession activa. Requerida para las operaciones MERGE.
                  Si no se provee, se obtiene la sesión activa con getOrCreate().
    silver_bucket : Bucket S3 Silver. Si None, lee S3_SILVER_BUCKET del entorno.
    """
    active_spark = spark or SparkSession.builder.getOrCreate()
    bucket = silver_bucket or require_env("S3_SILVER_BUCKET")
    writer = SilverWriter(silver_bucket=bucket, spark=active_spark)
    return writer.write(df, dataset_key)


# ---------------------------------------------------------------------------
# CLI de diagnóstico
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    # En modo CLI, lista las estrategias de particionado y las llaves de MERGE
    print("\n Estrategias de particionado Silver registradas:\n")
    print(f"  {'Dataset Key':<50} {'Partition Columns':<25} {'Merge Keys'}")
    print(f"  {'-'*50} {'-'*25} {'-'*40}")
    for key in PARTITION_STRATEGIES:
        parts_str = ", ".join(PARTITION_STRATEGIES[key]) if PARTITION_STRATEGIES[key] else "(sin partición)"
        merge_str = ", ".join(MERGE_KEYS.get(key, [])) or "(sin MERGE_KEYS)"
        print(f"  {key:<50} {parts_str:<25} {merge_str}")
    print()
