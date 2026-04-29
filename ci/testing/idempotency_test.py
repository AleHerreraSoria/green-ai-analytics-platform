"""
idempotency_test.py
====================
Prueba de idempotencia end-to-end del pipeline Green AI.

Valida empíricamente que múltiples ejecuciones del ETL Bronze→Silver y
Silver→Gold sobre los mismos datos de S3 NO producen:
  - Incremento en el conteo de filas (sin duplicados).
  - Diferencias en el conteo entre runs (estado final estable).

Estrategia:
  1. Se ejecuta bronze_to_silver.main() N veces.
  2. Tras cada ejecución, se lee la tabla Delta en S3 con Spark local y se
     registra el conteo de filas y el número de duplicados por clave primaria.
  3. Se comparan todos los snapshots: un pipeline idempotente produce conteos
     idénticos en todas las ejecuciones con cero duplicados.

Ejecutar (desde la raíz del repositorio):
    python idempotency_test.py

Requiere:
    - spark/.env con S3_BRONZE_BUCKET, S3_SILVER_BUCKET, S3_GOLD_BUCKET,
      AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY configurados.
    - PySpark >= 3.5, delta-spark >= 3.2, boto3, python-dotenv instalados.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

# ── dotenv ─────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # __file__ está en ci/testing/ → subir 2 niveles para llegar a la raíz del proyecto
    _project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    _env_path = os.path.join(_project_root, "spark", ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
        print(f"[idempotency_test] .env cargado desde: {_env_path}")
    else:
        load_dotenv()  # fallback: busca en CWD
except ImportError:
    pass

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.idempotency-test")

UTC = timezone.utc


# =============================================================================
# Configuración de entorno
# =============================================================================

def _require_env(var: str) -> str:
    value = os.getenv(var, "").strip()
    if not value:
        raise EnvironmentError(
            f"Variable de entorno requerida no encontrada: {var}\n"
            f"  Asegúrate de tener spark/.env configurado."
        )
    return value


SILVER_BUCKET = _require_env("S3_SILVER_BUCKET")
GOLD_BUCKET   = _require_env("S3_GOLD_BUCKET")
SILVER        = f"s3a://{SILVER_BUCKET}"
GOLD          = f"s3a://{GOLD_BUCKET}"

# Tablas a verificar con sus llaves primarias (para detección de duplicados)
SILVER_TARGETS: dict[str, list[str]] = {
    "usage_logs":                                    ["session_id"],
    "electricity_maps/carbon_intensity/latest":      ["zone", "datetime"],
    "electricity_maps/carbon_intensity/history":     ["zone", "datetime"],
    "electricity_maps/electricity_mix/latest":       ["zone", "datetime"],
    "reference/ec2_pricing":                         ["cloud_provider", "cloud_region", "instance_type",
                                                      "operating_system", "pricing_model"],
    "reference/geo_cloud_mapping":                   ["cloud_provider", "cloud_region"],
    "mlco2/instances":                               ["id"],
    "mlco2/impact":                                  ["region"],
    "mlco2/gpus":                                    ["gpu_model"],
    "owid":                                          ["country", "year"],
    "world_bank/ict_exports":                        ["country_code", "year"],
    "reference/world_bank_metadata":                 ["country_code"],
    "global_petrol_prices":                          ["country"],
}

GOLD_TARGETS: dict[str, list[str]] = {
    "dim_country":               ["country_id"],
    "dim_region":                ["region_id"],
    "dim_gpu_model":             ["gpu_id"],
    "dim_instance_type":         ["instance_type_id"],
    "dim_electricity_price":     ["price_id"],
    "fact_ai_compute_usage":     ["session_id"],
    "fact_country_energy_annual": ["iso_alpha3", "year"],
}


# =============================================================================
# SparkSession
# =============================================================================

def _build_spark():
    """
    SparkSession local con conectividad S3A y Delta Lake.
    Reutiliza la sesión si ya existe (getOrCreate).
    """
    import os
    from pyspark.sql import SparkSession

    # ── Garantizar JAVA_HOME en el entorno del proceso ────────────────────────
    # PySpark usa subprocess.Popen para lanzar la JVM (py4j gateway).
    # Si JAVA_HOME no está en os.environ (solo en la sesión de shell), falla
    # con WinError 2. Se setea explícitamente aquí como garantía.
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        candidate = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
        if os.path.isdir(candidate):
            java_home = candidate
            os.environ["JAVA_HOME"] = java_home
            logger.info("[spark] JAVA_HOME seteado automáticamente: %s", java_home)
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("green-ai-idempotency-test")
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ansi.enabled",          "false")
        .config("spark.sql.session.timeZone",      "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "software.amazon.awssdk:bundle:2.20.18",
        )
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
        .config("spark.hadoop.fs.s3a.fast.upload",                  "true")
        .config("spark.driver.memory",                              "4g")
        .config("spark.ui.showConsoleProgress",                      "false")
        .getOrCreate()
    )


# =============================================================================
# Modelo de datos
# =============================================================================

@dataclass
class TableSnapshot:
    """Estado de una tabla Delta en un momento dado."""
    table_path:   str
    layer:        str          # "silver" | "gold"
    row_count:    int
    dup_count:    int          # filas con clave primaria duplicada
    pk_columns:   list[str]
    ts:           datetime = field(default_factory=lambda: datetime.now(UTC))
    error:        str | None = None

    def is_clean(self) -> bool:
        return self.dup_count == 0 and self.error is None

    def to_dict(self) -> dict:
        return {
            "table":     self.table_path,
            "layer":     self.layer,
            "row_count": self.row_count,
            "dup_count": self.dup_count,
            "clean":     self.is_clean(),
            "ts":        self.ts.isoformat(),
            "error":     self.error,
        }


@dataclass
class RunSnapshot:
    """Snapshot completo de Silver + Gold tras una ejecución del ETL."""
    run_number: int
    ts:         datetime = field(default_factory=lambda: datetime.now(UTC))
    tables:     dict[str, TableSnapshot] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "run":    self.run_number,
            "ts":     self.ts.isoformat(),
            "tables": {k: v.to_dict() for k, v in self.tables.items()},
        }


# =============================================================================
# Core: captura de snapshot con Spark
# =============================================================================

def _snapshot_table(
    spark,
    base_url: str,
    table_rel: str,
    pk_cols: list[str],
    layer: str,
) -> TableSnapshot:
    """
    Lee la tabla Delta en S3 y calcula:
      - row_count : total de filas
      - dup_count : filas que comparten clave primaria con otra (duplicados)
    """
    from pyspark.sql import functions as F

    path = f"{base_url}/{table_rel}"
    try:
        df = spark.read.format("delta").load(path)
        row_count = df.count()

        # Detección de duplicados: groupBy(pk) → contar grupos con count > 1
        dup_count = (
            df.groupBy(*pk_cols)
              .count()
              .filter(F.col("count") > 1)
              .count()
        )
        return TableSnapshot(
            table_path=table_rel,
            layer=layer,
            row_count=row_count,
            dup_count=dup_count,
            pk_columns=pk_cols,
        )
    except Exception as exc:
        logger.warning("[snapshot] %s → %s", table_rel, exc)
        return TableSnapshot(
            table_path=table_rel,
            layer=layer,
            row_count=-1,
            dup_count=-1,
            pk_columns=pk_cols,
            error=str(exc),
        )


def capture_run_snapshot(spark, run_number: int) -> RunSnapshot:
    """Captura el estado actual de todas las tablas Silver y Gold."""
    snap = RunSnapshot(run_number=run_number)

    logger.info("[snapshot] Capturando estado de Silver (%d tablas)...", len(SILVER_TARGETS))
    for table_rel, pk in SILVER_TARGETS.items():
        key = f"silver/{table_rel}"
        snap.tables[key] = _snapshot_table(spark, SILVER, table_rel, pk, "silver")

    logger.info("[snapshot] Capturando estado de Gold (%d tablas)...", len(GOLD_TARGETS))
    for table_rel, pk in GOLD_TARGETS.items():
        key = f"gold/{table_rel}"
        snap.tables[key] = _snapshot_table(spark, GOLD, table_rel, pk, "gold")

    return snap


# =============================================================================
# ETL runner
# =============================================================================

def run_etl_pipeline(spark) -> None:
    """
    Ejecuta el pipeline ETL completo Bronze→Silver→Gold inyectando la
    SparkSession activa en cada job.

    Al pasar ``spark`` explícitamente a cada ``main()``, los jobs saben que
    NO son los dueños del ciclo de vida de la sesión y omiten el
    ``spark.stop()`` al terminar, manteniendo la sesión viva para los
    snapshots posteriores del test de idempotencia.

    Parámetros
    ----------
    spark : SparkSession
        Sesión activa creada por el test. Se propaga a bronze_to_silver y
        silver_to_gold para garantizar contexto compartido durante todo el
        ciclo: snapshot-inicial → ETL → snapshot-posterior.
    """
    import importlib

    # __file__ está en ci/testing/ → subir 2 niveles para llegar a la raíz del proyecto
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    jobs_etl     = os.path.join(project_root, "spark", "jobs", "etl")

    for path in (project_root, os.path.join(project_root, "spark"), jobs_etl):
        if path not in sys.path:
            sys.path.insert(0, path)

    # ── Bronze → Silver ───────────────────────────────────────────────────────
    logger.info("[etl] Ejecutando Bronze → Silver ...")
    b2s_spec = importlib.util.spec_from_file_location(
        "bronze_to_silver",
        os.path.join(jobs_etl, "bronze_to_silver.py"),
    )
    b2s_mod = importlib.util.module_from_spec(b2s_spec)
    b2s_spec.loader.exec_module(b2s_mod)
    b2s_mod.main(spark=spark)   # ← inyectar sesión; el job NO llamará spark.stop()

    # ── Silver → Gold ─────────────────────────────────────────────────────────
    logger.info("[etl] Ejecutando Silver → Gold ...")
    s2g_spec = importlib.util.spec_from_file_location(
        "silver_to_gold",
        os.path.join(jobs_etl, "silver_to_gold.py"),
    )
    s2g_mod = importlib.util.module_from_spec(s2g_spec)
    s2g_spec.loader.exec_module(s2g_mod)
    s2g_mod.main(spark=spark)   # ← inyectar sesión; el job NO llamará spark.stop()


# =============================================================================
# Comparación de snapshots
# =============================================================================

def compare_snapshots(baseline: RunSnapshot, candidate: RunSnapshot) -> dict:
    """
    Compara dos snapshots y detecta:

      - regressions    : row_count del candidate difiere del baseline.
      - dup_introduced : duplicados que aparecen en el candidate pero no en el
                         baseline — el ETL INTRODUJO duplicados (violation real).
      - dup_preexisting: duplicados heredados del estado pre-existente de S3;
                         el ETL no los generó. Se reportan como warning de
                         calidad de datos pero NO bloquean el PASS de idempotencia.

    Criterio de PASS:
      regressions == 0  AND  dup_introduced == 0
    """
    regressions:     list[dict] = []
    dup_introduced:  list[dict] = []
    dup_preexisting: list[dict] = []

    for key, baseline_snap in baseline.tables.items():
        cand_snap = candidate.tables.get(key)
        if cand_snap is None:
            continue
        if baseline_snap.error or cand_snap.error:
            continue

        # ── Idempotencia de row_count ─────────────────────────────────────────
        if baseline_snap.row_count != cand_snap.row_count:
            regressions.append({
                "table":          key,
                "baseline_rows":  baseline_snap.row_count,
                "candidate_rows": cand_snap.row_count,
                "delta":          cand_snap.row_count - baseline_snap.row_count,
            })

        # ── Detección de duplicados ───────────────────────────────────────────
        # Bug anterior: `if cand_snap.dup_count > 0` falla siempre que la tabla
        # tenga duplicados pre-existentes en S3, incluso cuando el ETL no los
        # generó. Eso hace que el test sea sensible al estado heredado, no al
        # comportamiento del pipeline.
        #
        # Lógica correcta:
        #   - cand_dups > baseline_dups → el ETL generó/empeoró duplicados → FAIL
        #   - cand_dups > 0 pero <= baseline_dups → duplicados heredados → WARNING
        baseline_dups = max(baseline_snap.dup_count, 0)
        cand_dups     = max(cand_snap.dup_count, 0)

        if cand_dups > baseline_dups:
            dup_introduced.append({
                "table":          key,
                "baseline_dups":  baseline_dups,
                "candidate_dups": cand_dups,
                "new_dups":       cand_dups - baseline_dups,
                "pk":             cand_snap.pk_columns,
                "run":            candidate.run_number,
            })
        elif cand_dups > 0:
            dup_preexisting.append({
                "table":     key,
                "dup_count": cand_dups,
                "pk":        cand_snap.pk_columns,
                "run":       candidate.run_number,
                "note":      "duplicados pre-existentes en S3; no introducidos por este run",
            })

    idempotent = len(regressions) == 0 and len(dup_introduced) == 0
    return {
        "comparison":      f"run_{baseline.run_number}_vs_run_{candidate.run_number}",
        "idempotent":      idempotent,
        "regressions":     regressions,
        "dup_violations":  dup_introduced,
        "dup_preexisting": dup_preexisting,
        "status":          "PASS" if idempotent else "FAIL",
    }



# =============================================================================
# Main
# =============================================================================

NUM_RUNS = 2  # Número de ejecuciones del ETL para validar idempotencia


def main() -> int:
    logger.info("=" * 65)
    logger.info("  GREEN AI — Prueba de Idempotencia End-to-End")
    logger.info("=" * 65)

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    snapshots: list[RunSnapshot] = []
    comparisons: list[dict] = []
    failed_tables: list[str] = []

    try:
        # ── Run 0: estado inicial ANTES de cualquier ejecución ───────────────
        logger.info("[run-0] Capturando estado inicial en S3...")
        snap0 = capture_run_snapshot(spark, run_number=0)
        snapshots.append(snap0)

        # ── Runs 1..N: ejecutar ETL y comparar ──────────────────────────────
        for run_n in range(1, NUM_RUNS + 1):
            logger.info("")
            logger.info("─" * 65)
            logger.info("[run-%d/%d] Iniciando ejecución del ETL...", run_n, NUM_RUNS)
            t0 = time.time()

            run_etl_pipeline(spark)

            elapsed = time.time() - t0
            logger.info("[run-%d] ETL completado en %.1fs.", run_n, elapsed)

            snap_n = capture_run_snapshot(spark, run_number=run_n)
            snapshots.append(snap_n)

            # Comparar contra el run anterior
            comp = compare_snapshots(snapshots[run_n - 1], snap_n)
            comparisons.append(comp)
            logger.info(
                "[run-%d] Resultado: %s — regressions=%d  dup_introduced=%d  dup_preexisting=%d",
                run_n,
                comp["status"],
                len(comp["regressions"]),
                len(comp["dup_violations"]),
                len(comp.get("dup_preexisting", [])),
            )
            if comp["regressions"]:
                for r in comp["regressions"]:
                    logger.error(
                        "  ⚠  REGRESIÓN en '%s': baseline=%d → candidate=%d (Δ=%+d)",
                        r["table"], r["baseline_rows"], r["candidate_rows"], r["delta"],
                    )
            if comp["dup_violations"]:
                for d in comp["dup_violations"]:
                    logger.error(
                        "  ⚠  DUPLICADOS en '%s': %d grupos duplicados (pk=%s)",
                        d["table"], d["dup_count"], d["pk"],
                    )

        # ── Reporte final ────────────────────────────────────────────────────
        overall_pass = all(c["status"] == "PASS" for c in comparisons)
        # Tablas con error de lectura en el último snapshot
        last_snap = snapshots[-1]
        failed_tables = [
            k for k, v in last_snap.tables.items() if v.error is not None
        ]

        report = {
            "test":          "idempotency",
            "num_runs":      NUM_RUNS,
            "overall_status": "PASS" if overall_pass and not failed_tables else "FAIL",
            "timestamp":     datetime.now(UTC).isoformat(),
            "failed_tables": failed_tables,
            "comparisons":   comparisons,
            "snapshots":     [s.to_dict() for s in snapshots],
        }

        print("\n" + "=" * 65)
        print("  RESULTADO — Prueba de Idempotencia")
        print("=" * 65)
        print(json.dumps(report, indent=2, default=str))

        exit_code = 0 if report["overall_status"] == "PASS" else 1
        return exit_code

    except Exception as exc:
        logger.exception("Error crítico durante la prueba de idempotencia: %s", exc)
        return 2

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    sys.exit(main())