# Green AI Analytics Platform

Plataforma de datos orientada a una ONG para analizar el **impacto ambiental y económico** de la inteligencia artificial en América, combinando datos históricos (energía, economía), catálogo de hardware (MLCO2), **intensidad de carbono en tiempo (casi) real** (Electricity Maps) y **logs sintéticos** de sesiones de cómputo que simulan movilidad regional de cargas de trabajo.

**Carrera:** Data Engineering · **Enfoque:** sostenibilidad digital, economía y movilidad de datos en América.

> Este README describe **lo implementado en el repositorio** a la fecha. Para detalle por capa, arquitectura y despliegue, el índice maestro está en [`docs/README.md`](docs/README.md).

---

## Estado actual del proyecto

| Área | Implementado |
|------|----------------|
| **Infraestructura (AWS)** | Terraform: buckets S3 **Bronze, Silver, Gold** y **sources** (`sources-green-ai`), grupo de seguridad, instancias EC2 (**Airflow**, **Spark**, **Control Tower**) con IPs elásticas, usuarios IAM del equipo y usuario de solo lectura Gold para serving. Ver `infrastructure/terraform/`. |
| **Capa Bronze** | Ingesta vía scripts en `airflow/scripts/`, copia desde bucket **sources** al inicio del DAG E2E, datos de referencia y logs. Documentación: [`docs/pipelines/bronze/bronze_layer.md`](docs/pipelines/bronze/bronze_layer.md). |
| **Silver / Gold** | Jobs **PySpark** en `spark/jobs/etl/` (`bronze_to_silver.py`, `silver_to_gold.py`), validaciones en `spark/jobs/quality/`, librerías en `spark/libs/`. Orquestados desde Airflow por SSH al nodo Spark. |
| **Orquestación** | Airflow (Celery) en `airflow/docker/docker-compose.yml`. DAG principal `green-ai-full-pipeline` en `airflow/dags/pipeline/end_to_end_pipeline.py`; DAGs de soporte en `airflow/dags/support/` (`dag_bronze`, `dag_silver`, `dag_gold`). |
| **Visualización y observabilidad** | **Control Tower** (Streamlit): storytelling, pipeline live frente a la API de Airflow, catálogo S3, analytics sobre Gold y **módulo ML** en `control-tower/ML/`. Carpeta **`dashboard/`**: app Streamlit adicional orientada a páginas analíticas. |
| **Calidad y CI** | `tests/` (pytest), `ci/smoke_checks.py`, **GitHub Actions** (flake8, `compileall`, smoke) en `.github/workflows/ci.yml`; despliegue por carpetas a EC2 en `.github/workflows/deploy-ec2-by-folder.yml`. |

---

## Visión y preguntas de negocio

La propuesta busca responder, entre otras, optimización regional (ahorro de CO₂ al mover entrenamiento entre países y “horas verdes”), eficiencia de hardware frente a coste eléctrico y huella, correlación PIB/tecnología vs limpieza de la red, y proyecciones de demanda energética. KPIs y supuestos: [`docs/architecture/business_questions.md`](docs/architecture/business_questions.md) y [`docs/architecture/business_requirements.md`](docs/architecture/business_requirements.md).

---

## Fuentes de datos (rol vs repositorio)

| Fuente | Rol | En este repo |
|--------|-----|---------------|
| Our World in Data (energía) | Contexto histórico | En **sources**: `Our_World_In_Data/owid-energy-data.csv`; el DAG lo replica a Bronze como `owid/owid-energy-data.csv`. Obtener el CSV localmente bajo `data/` y subirlo con `upload_sources_to_s3.py`. |
| Electricity Maps API | Intensidad de carbono y mix | `airflow/scripts/ingest_electricity_maps.py` (token en `.env`). |
| MLCO2 (hardware) | GPUs, instancias, impacto | En **sources** bajo `Code_Carbon/`; mapeo a prefijos `mlco2/` en Bronze definido en el DAG E2E. |
| Logs sintéticos | Sesiones de uso IA | `airflow/scripts/generate_synthetic_usage_logs.py` (salida típica bajo `data/` o rutas usadas por el pipeline). |
| Precios electricidad, World Bank, referencia EC2, geo zonas | Costes y joins | Rutas en **sources** según el mapeo del DAG; referencia EC2 regenerable con el script de pricing. |

Contratos y definiciones: [`docs/references/data_dictionary.md`](docs/references/data_dictionary.md), [`docs/DICCIONARIO_DE_DATOS_GOLD.md`](docs/DICCIONARIO_DE_DATOS_GOLD.md) (Gold). Flujo **sources → Bronze**: documentado en [`airflow/README.md`](airflow/README.md) y en el manual de operación bajo [`docs/setup_and_deployment/`](docs/setup_and_deployment/).

---

## Requisitos

- **Python 3.10+** y `pip` (por componente; no hay un único `requirements.txt` en la raíz).
- **Cuenta AWS** con permisos S3 (Bronze, Silver, Gold, **sources**), credenciales vía perfil o variables de entorno; en producción se recomienda IAM roles en EC2 en lugar de claves en disco.
- **Docker / Docker Compose** para Airflow local y opcionalmente Control Tower.
- **Token** de [Electricity Maps](https://www.electricitymaps.com/) para la ingesta API.
- **Spark** (`spark-submit`) en la instancia o entorno donde se ejecuten los jobs (ver [`spark/README.md`](spark/README.md)).

### Dependencias por carpeta

| Ubicación | Instalación típica |
|-----------|---------------------|
| Airflow (imagen local) | Definidas en `airflow/requirements/requirements-airflow.txt` (build de la imagen Docker). |
| Control Tower | `pip install -r control-tower/requirements.txt` (+ `control-tower/ML/requirements.txt` si usas el pipeline ML). |
| Dashboard standalone | `pip install -r dashboard/requirements.txt`. |
| Spark (libs/jobs) | `spark/requirements/requirements-spark.txt` en el entorno Spark. |

---

## Configuración rápida

1. **Clonar** el repositorio.

2. **Variables de entorno** (no versionar secretos reales):
   - Raíz: **`.env.example`** — contrato compartido (buckets, Airflow API para Control Tower, etc.). Copia a `.env` según el entorno.
   - Airflow: detalles adicionales en **`airflow/.env.example`** (`AIRFLOW_BASE_URL` sin barra final, `AIRFLOW_WEBSERVER_SECRET_KEY`, `AIRFLOW_SSH_PRIVATE_KEY_PATH`, `S3_SOURCES_BUCKET`, `SPARK_SUBMIT_BIN`, `SPARK_SUBMIT_EXTRA_ARGS`, etc.).
   - Spark: **`spark/.env.example`**.
   - Control Tower: **`control-tower/.env.example`** (API de Airflow, `PIPELINE_DAG_ID`, credenciales).

3. **Datos locales**: carpeta **`data/`** (en `.gitignore`) con el layout esperado por `airflow/scripts/upload_sources_to_s3.py` para poblar el bucket **sources** antes de ejecutar el pipeline E2E.

4. **`PIPELINE_DAG_ID`** (Control Tower / integraciones) debe coincidir con el DAG orquestado en tu Airflow; en el código del repo el pipeline principal es **`green-ai-full-pipeline`**.

---

## Scripts principales (`airflow/scripts/`)

| Script | Descripción |
|--------|-------------|
| `generate_synthetic_usage_logs.py` | Genera CSV de sesiones de uso IA alineado al diccionario de datos. |
| `ingest_electricity_maps.py` | Descarga Electricity Maps → JSON en disco o S3 bajo `electricity_maps/`. |
| `upload_sources_to_s3.py` | Sube el contenido de `data/` al bucket **sources** (layout de negocio). |
| `build_aws_ec2_pricing_reference.py` | Regenera referencia de precios EC2 para uso local / referencia. |

Ejemplos (desde la raíz del repo, con el entorno Python adecuado e instalación de dependencias):

```bash
python airflow/scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
python airflow/scripts/ingest_electricity_maps.py --mode latest
python airflow/scripts/upload_sources_to_s3.py --bucket TU_BUCKET_SOURCES
```

---

## Spark (Medallion)

- **ETL:** `spark/jobs/etl/bronze_to_silver.py`, `spark/jobs/etl/silver_to_gold.py`.
- **Calidad:** `bronze_validations.py`, `silver_validations.py`, `gold_validations.py`, `audit.py`.
- Ejecución y variables: [`spark/README.md`](spark/README.md).

---

## Control Tower y dashboard

- **Control Tower** (`control-tower/`): Streamlit multi-página (`main.py`, `pages/`), componentes en `components/`, utilidades S3/Airflow en `utils/`, analytics Gold en `analytics_dashboard/`, **ML** en `ML/` (entrenamiento, scoring, export de metadatos Airflow). Guía de equipo: [`control-tower/README.md`](control-tower/README.md). Docker: `control-tower/docker-compose.yml` (puerto típico **8501**).
- **Dashboard** (`dashboard/`): aplicación Streamlit independiente con páginas analíticas; `pip install -r dashboard/requirements.txt` y ejecución con `streamlit run dashboard/app.py` según tu entorno.

---

## Infraestructura (Terraform)

Ubicación: `infrastructure/terraform/`.

- Cuatro buckets: **Bronze, Silver, Gold** (nombres con sufijo aleatorio) y **sources** (`sources-green-ai`, recurso importado/gestionado según tu estado).
- Security group con **22** y **8080** (ajustar CIDR en producción).
- EC2 **Airflow** y **Spark** (`m7i-flex.large`) con Elastic IP cada una; instancia **Control Tower** y EIP asociada (configuración concreta en `main.tf`).
- Usuario IAM **solo lectura** sobre Gold para herramientas de visualización.

Inicializar y aplicar con `terraform init` / `terraform apply` desde esa carpeta. Revisar **outputs** para nombres de bucket e IPs.

---

## Airflow (local)

Desde la raíz del repositorio:

```bash
docker compose --env-file .env -f airflow/docker/docker-compose.yml up -d
```

Interfaz en el puerto **8080** (o `AIRFLOW_WEBSERVER_PORT`). Usuario/contraseña: `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`.

**Conexiones obligatorias en la UI de Airflow:** `aws_default` (S3), `spark_ssh` (ejecución remota de jobs). Requisitos y variables detalladas: [`airflow/README.md`](airflow/README.md).

---

## Pruebas y CI local

```bash
pip install pytest flake8
pytest tests/
python ci/smoke_checks.py
flake8 . --count --select=F63,F7,F82 --show-source --statistics
```

En CI (GitHub Actions) se ejecutan comprobaciones similares; ver `.github/workflows/ci.yml`.

---

## Estructura del repositorio (resumen)

```
green-ai-analytics-platform/
├── airflow/                 # Docker Compose, DAGs, scripts de ingesta
├── spark/                   # Jobs ETL Medallion y validaciones
├── control-tower/           # Streamlit: observabilidad, analytics, ML
├── dashboard/               # Streamlit: páginas analíticas standalone
├── infrastructure/terraform/
├── docs/                    # Índice: docs/README.md
├── ci/                      # Smoke checks y pruebas de resiliencia
├── tests/                   # pytest
├── utils/                   # Diagramas y utilidades compartidas
└── data/                    # Datos locales (gitignored)
```

---

## Documentación adicional

El índice organizado por arquitectura, pipelines Medallion, despliegue y referencias está en **[`docs/README.md`](docs/README.md)**. Destacados:

- Arquitectura: `docs/architecture/` (decisiones técnicas, modelo dimensional, streaming propuesto).
- Pipelines: `docs/pipelines/bronze|silver|gold/`.
- Despliegue: `docs/setup_and_deployment/` (infraestructura, manual de operación).
- Referencias: `docs/references/data_dictionary.md`, `docs/references/glossary_metrics.md`.

> Algunos comentarios en código aún citan `docs/DICCIONARIO_DE_DATOS.md`; el contenido consolidado de contratos vive en **`docs/references/data_dictionary.md`**.

---

## Contrato operativo mínimo

- Docker activo antes de levantar Airflow; variables obligatorias sin fallback donde el compose las exige (`:?`).
- Bucket **sources** poblado y accesible; `S3_SOURCES_BUCKET` alineado con Terraform/operación.
- Buckets Bronze/Silver/Gold creados y accesibles desde Airflow y Spark.
- Clave SSH montada en el contenedor Airflow (`AIRFLOW_SSH_PRIVATE_KEY_PATH`) para `spark_ssh`.
- `AIRFLOW_BASE_URL` coincidente con la URL real del navegador (host y puerto, **sin barra final**).
- Para Control Tower en modo “pipeline live”: URL de API Airflow HTTPS y credenciales de servicio; `PIPELINE_DAG_ID` coherente con el DAG desplegado.

---

## Entornos

Gestionar variables por entorno con archivos separados (por ejemplo `.env.dev`, `.env.staging`, `.env.prod`) y no compartir secretos. Usar los `.env.example` de cada componente como plantilla.

---

## Próximos pasos (evolutivos)

- Endurecer reglas de security group (CIDR restringido, TLS donde aplique).
- Alinear documentación histórica (`manual_de_operación.md`) con nombres actuales de DAGs si difieren del código.
- Ampliar cobertura de tests automatizados sobre reglas de negocio y contratos de datos.

---

## Licencia y propósito académico

Proyecto académico de **Data Engineering**. Revisa las licencias de los datasets externos (Our World in Data, MLCO2, Electricity Maps, Banco Mundial, etc.) antes de redistribuir datos o derivados.
