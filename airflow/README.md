# Airflow Instance Guide

Este README describe como levantar y operar la instancia de Airflow de este proyecto.

## Estructura relevante

- `docker/docker-compose.yml`: stack local de Airflow (webserver, scheduler, worker, postgres, redis).
- `dags/`: DAGs versionados del pipeline E2E y DAGs de soporte.
- `.env.example`: contrato de variables para esta instancia.

## Prerrequisitos

- Docker Desktop / Docker daemon activo.
- Archivo `.env` en la raiz del repo con variables requeridas.
- Conexiones en Airflow:
  - `aws_default` (acceso S3).
  - `spark_ssh` (ejecucion remota de jobs Spark).
- Buckets S3 de Bronze/Silver/Gold existentes y con datos base para la corrida E2E.

## Variables de entorno

Usa `airflow/.env.example` como referencia minima.

Variables clave:

- Seguridad y acceso: `AIRFLOW__CORE__FERNET_KEY`, `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`.
- DB/Celery: `POSTGRES_*`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CELERY__RESULT_BACKEND`.
- Config DAGs: `AIRFLOW_AWS_CONN_ID`, `SPARK_SSH_CONN_ID`, `SPARK_REPO_PATH`, `AIRFLOW_SCRIPTS_PATH`, `AIRFLOW_DATA_PATH`.
- Datos: `S3_BRONZE_BUCKET`, `S3_SILVER_BUCKET`, `S3_GOLD_BUCKET`.

## Arranque local

Desde la raiz del repo:

```bash
docker compose --env-file .env -f airflow/docker/docker-compose.yml up -d
```

Interfaz web:

- URL: `http://localhost:8080`
- Usuario/password: `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`

## Operacion basica

- Ver logs de servicios:

```bash
docker compose --env-file .env -f airflow/docker/docker-compose.yml logs -f
```

- Bajar stack:

```bash
docker compose --env-file .env -f airflow/docker/docker-compose.yml down
```

## Notas de hardening aplicadas

- Sin defaults inseguros para credenciales ni `FERNET_KEY`.
- Sin `redis:latest` (tag pinneado).
- DAGs con configuracion centralizada y variables obligatorias (fail-fast).
