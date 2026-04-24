# Spark Instance Guide

Este README describe como configurar y ejecutar los jobs Spark de la plataforma.

## Estructura relevante

- `jobs/etl/`: transformaciones Bronze -> Silver y Silver -> Gold.
- `jobs/quality/`: validaciones y auditoria de capas.
- `libs/`: utilidades compartidas (schemas, writer, etc).
- `.env.example`: contrato de variables para esta instancia.

## Prerrequisitos

- Spark y `spark-submit` disponibles en la instancia.
- Permisos AWS via IAM Role o provider chain equivalente.
- Buckets S3 de Bronze/Silver/Gold configurados y accesibles.
- Variables de entorno requeridas exportadas antes de ejecutar jobs.

## Variables de entorno

Usa `spark/.env.example` como referencia.

Obligatorias:

- `S3_BRONZE_BUCKET`
- `S3_SILVER_BUCKET`
- `S3_GOLD_BUCKET`

Recomendadas segun entorno:

- `AWS_DEFAULT_REGION`
- `APP_ENV`

## Ejecucion de jobs

Ejemplos:

```bash
spark-submit spark/jobs/etl/bronze_to_silver.py
spark-submit spark/jobs/quality/silver_validations.py --fail
spark-submit spark/jobs/etl/silver_to_gold.py
spark-submit spark/jobs/quality/gold_validations.py --fail
```

## Operacion desde Airflow remoto

Los DAGs Airflow usan `SSHOperator` con la conexion `spark_ssh` y exportan variables de buckets al ejecutar:

- `jobs/etl/bronze_to_silver.py`
- `jobs/quality/silver_validations.py`
- `jobs/quality/audit.py`
- `jobs/etl/silver_to_gold.py`
- `jobs/quality/gold_validations.py`

## Notas de hardening aplicadas

- Sin defaults hardcodeados a buckets productivos.
- Fail-fast si faltan variables de entorno criticas.
- Sin `spark.hadoop.fs.s3a.access.key`/`secret.key` en codigo.
- Autenticacion S3 via provider chain / IAM role.
