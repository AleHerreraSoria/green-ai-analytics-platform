# Green AI Analytics Platform

Plataforma de datos orientada a una ONG para analizar el **impacto ambiental y económico** de la inteligencia artificial en América, combinando datos históricos (energía, economía), catálogo de hardware (MLCO2), **intensidad de carbono en tiempo (casi) real** (Electricity Maps) y **logs sintéticos** de sesiones de cómputo que simulan movilidad regional de cargas de trabajo.

**Carrera:** Data Engineering · **Enfoque:** sostenibilidad digital, economía y movilidad de datos en América.

> Este README describe **solo lo que ya está implementado** en el repositorio. A medida que se añadan capas Silver/Gold, orquestación con DAGs, visualización, etc., conviene actualizar esta sección.

---

## Estado actual del proyecto

| Área | Implementado |
|------|----------------|
| **Infraestructura (AWS)** | Terraform: tres buckets S3 (Bronze, Silver, Gold), grupo de seguridad, instancia EC2 con IP elástica. Backend de estado en S3 (ver `infrastructure/terraform/providers.tf`). |
| **Capa Bronze (ingesta y datos locales)** | Scripts Python para logs sintéticos, ingesta Electricity Maps, referencia de precios EC2 aproximados y carga a S3. CSV de referencia y logs bajo `bronze/`. Documentación en `docs/`. |
| **Orquestación** | `docker-compose.yml` con el stack oficial de **Apache Airflow 2.8** (Celery + Postgres + Redis) para desarrollo local. **No hay DAGs versionados aún** en este repo (la carpeta `dags/` es la que monta Compose cuando exista). |
| **Silver / Gold** | Estructura de buckets preparada; transformaciones Medallion (PySpark, tablas analíticas) **pendientes**. |
| **Visualización** | **Pendiente** (p. ej. Power BI o Streamlit según la propuesta). |

---

## Visión y preguntas de negocio (objetivo)

La propuesta del proyecto final busca responder, entre otras, optimización regional (ahorro de CO₂ al mover entrenamiento entre países y “horas verdes”), eficiencia de hardware frente a coste eléctrico y huella, correlación PIB/tecnología vs limpieza de la red, y proyecciones de demanda energética. El detalle de KPIs y supuestos está en `docs/PREGUNTAS_DE_NEGOCIO.md`.

---

## Fuentes de datos (previstas vs repo)

| Fuente | Rol en la propuesta | En este repo |
|--------|---------------------|--------------|
| Our World in Data (energía) | Contexto histórico por país | Script de subida espera `data/Our_World_In_Data/owid-energy-data.csv` (carpeta `data/` ignorada por Git; hay que obtener el dataset localmente). |
| Electricity Maps API | Intensidad de carbono y mix | `scripts/ingest_electricity_maps.py` (token en `.env`). |
| MLCO2 (hardware) | GPUs, instancias, impacto regional | Generador de logs y subida esperan `data/Code_Carbon/*.csv` (no versionados). |
| Logs sintéticos | Sesiones de uso IA | `scripts/generate_synthetic_usage_logs.py` → `bronze/usage_logs/usage_logs.csv`. |
| Referencias extra (precios electricidad, World Bank, precios EC2) | Apoyo a costes y joins | Subida desde rutas bajo `data/…` según `scripts/upload_bronze_to_s3.py`; `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` se puede regenerar con el script de pricing. |

Contrato de columnas y mapeos: `docs/DICCIONARIO_DE_DATOS.md`. Operativa Bronze: `docs/DOCUMENTACION_CAPA_BRONZE.md`.

---

## Requisitos

- **Python 3.10+** (recomendado) y `pip`.
- **Cuenta AWS** con permisos para S3 (y lo que defina tu Terraform), credenciales vía perfil o variables de entorno.
- **Docker / Docker Compose** (solo si vas a levantar Airflow local).
- **Token** de [Electricity Maps](https://www.electricitymaps.com/) para la ingesta API.

---

## Configuración rápida

1. Clonar el repositorio e instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```

2. Crear un archivo **`.env`** en la raíz (no se versiona). Ejemplo mínimo:

   ```env
   ELECTRICITY_MAPS_TOKEN=tu_token
   AWS_S3_BUCKET=nombre-del-bucket-bronze
   # Opcional: AWS_PROFILE, AWS_DEFAULT_REGION
   ```

3. Colocar los CSV externos bajo **`data/`** según las rutas esperadas por `scripts/upload_bronze_to_s3.py` y por el generador de logs (MLCO2). La carpeta `data/` está en `.gitignore`.

---

## Scripts principales

| Script | Descripción |
|--------|-------------|
| `scripts/generate_synthetic_usage_logs.py` | Genera CSV de sesiones de uso IA alineado al diccionario de datos (regiones MLCO2, movilidad por usuario, casos borde opcionales). |
| `scripts/ingest_electricity_maps.py` | Descarga datos de Electricity Maps y escribe JSON en disco o en S3 bajo el prefijo `electricity_maps/`. |
| `scripts/upload_bronze_to_s3.py` | Sube al bucket Bronze los CSV locales (referencia, usage_logs, y los que existan en `data/`). |
| `scripts/build_aws_ec2_pricing_reference.py` | Regenera `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` a partir de catálogos locales. |

Ejemplos:

```bash
python scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
python scripts/ingest_electricity_maps.py --mode latest
python scripts/upload_bronze_to_s3.py --bucket TU_BUCKET_BRONZE
```

---

## Infraestructura (Terraform)

Ubicación: `infrastructure/terraform/`.

- Tres buckets S3 con nombre derivado de `var.project_name` y un sufijo aleatorio (Bronze, Silver, Gold).
- Security group con puertos **22** y **8080** abiertos a `0.0.0.0/0` (ajustar en entornos reales).
- Instancia **EC2** (`m7i-flex.large`) con `key_name` configurado en el código Terraform y **Elastic IP**.

Inicializar y aplicar según tu backend y variables (por ejemplo `terraform init` y `terraform apply` desde esa carpeta). Los nombres de bucket reales salen en los **outputs** de Terraform; úsalos para `AWS_S3_BUCKET` y documentación operativa.

---

## Airflow (local)

Desde la raíz del proyecto (donde está `docker-compose.yml`):

```bash
docker compose up -d
```

Interfaz web por defecto en el puerto **8080** (usuario/contraseña según variables de entorno de Compose; ver comentarios al inicio del YAML). Cuando existan DAGs en `./dags`, quedarán montados en el contenedor.

---

## Estructura del repositorio (resumen)

```
green-ai-analytics-platform/
├── bronze/                 # Artefactos Bronze locales (usage_logs, reference)
├── docs/                   # Diccionario de datos, Bronze, preguntas de negocio, decisiones
├── infrastructure/terraform/
├── scripts/                # Ingesta, generación y subida a S3
├── docker-compose.yml      # Stack Airflow para desarrollo
└── requirements.txt
```

---

## Documentación adicional

- `docs/DICCIONARIO_DE_DATOS.md` — contratos y definiciones.
- `docs/DOCUMENTACION_CAPA_BRONZE.md` — organización del bucket y flujos de ingesta.
- `docs/PREGUNTAS_DE_NEGOCIO.md` — KPIs y análisis previstos.
- `docs/technical-decisions.md` — decisiones técnicas del equipo.

---

## Próximos pasos sugeridos (para completar el README con el tiempo)

- DAGs de Airflow que encadenen generación/subida e ingesta programada.
- Capas **Silver** y **Gold** (limpieza, joins, agregados).
- **Dashboard** o app de visualización.
- Endurecimiento de seguridad en Terraform (CIDR, RDS si aplica a la arquitectura final, etc.).

---

## Licencia y propósito académico

Proyecto académico de **Data Engineering**. Revisa las licencias de los datasets externos (Our World in Data, MLCO2, Electricity Maps, Banco Mundial, etc.) antes de redistribuir datos o derivados.
