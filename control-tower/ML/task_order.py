"""
Orden de tareas del DAG green-ai-full-pipeline (índice para features de orden).
Debe alinearse con airflow/dags/pipeline/end_to_end_pipeline.py.
"""

from __future__ import annotations

PIPELINE_TASK_IDS: tuple[str, ...] = (
    "sync_sources_bucket_to_bronze",
    "build_aws_ec2_pricing_reference",
    "generate_usage_logs",
    "ingest_electricity_maps_api",
    "validate_bronze_quality_remote",
    "transform_bronze_to_silver_remote",
    "validate_silver_quality_remote",
    "audit_silver_data_remote",
    "build_kimball_gold_layer_remote",
    "validate_gold_quality_remote",
)


def task_index(task_id: str) -> int:
    try:
        return PIPELINE_TASK_IDS.index(task_id)
    except ValueError:
        return len(PIPELINE_TASK_IDS)
