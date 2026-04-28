# 🌿 Green AI Analytics Platform

> **Ecosistema de Datos para la Gobernanza de Carbono y Optimización de Costos en AWS**

Este repositorio contiene la implementación integral de una plataforma de datos diseñada para monitorear, auditar y optimizar la huella de carbono y los costos operativos de infraestructuras basadas en Inteligencia Artificial dentro de AWS.

---

## 🏗️ Arquitectura y Decisiones Técnicas

La plataforma se basa en una **Arquitectura de Medallón** (Bronze, Silver, Gold) sobre un Data Lake en S3, orquestada por  **Apache Airflow** .

* **Procesamiento:** Spark distribuido para limpieza y normalización.
* **Modelado:** Esquema Estrella (Kimball) en la capa Gold para analítica de alta performance.
* **IaC:** 100% de los recursos (incluyendo la Control Tower y el Bucket de Fuentes) están gestionados mediante  **Terraform** , garantizando la inmutabilidad y reproducibilidad del entorno.

---

## 🛠️ Pre-requisitos de Software

Para asegurar una correcta reproducción del entorno, verifique contar con las siguientes versiones:

* **Terraform:** `v1.5.0` o superior.
* **AWS CLI:** `v2.x` (configurado con credenciales de `AdministratorAccess`).
* **Docker & Docker Compose:** `v20.10+` / `v2.x` (en la instancia de Airflow).
* **Python:** `v3.9+`.
* **Git:** Para la clonación y gestión de ramas.

---

## 🚀 Guía de Despliegue Paso a Paso

### 1. Preparación del Entorno

Clone el repositorio y sitúese en la carpeta de infraestructura:

**Bash**

```
git clone https://github.com/AleHerreraSoria/green-ai-analytics-platform.git
cd green-ai-analytics-platform/infrastructure/terraform
```

### 2. Despliegue de Infraestructura (IaC)

El proyecto utiliza un **Remote Backend** para el estado de Terraform. Asegúrese de tener permisos de escritura en el bucket de estado definido en `providers.tf`.

**Bash**

```
terraform init
terraform plan
terraform apply -auto-approve
```

> **Nota técnica:** Al finalizar, el comando generará `outputs` críticos como las IPs públicas de los servidores y las claves de acceso para el Dashboard (Serving).

### 3. Configuración de Variables de Entorno (`.env`)

En la raíz del proyecto, cree un archivo `.env` basado en `.env.example`. Este archivo es consumido por Docker y los procesos de Spark:

**Fragmento de código**

```
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key
ELECTRICITY_MAPS_TOKEN=tu_token_de_api
S3_BRONZE_BUCKET=green-ai-pf-bronze-[suffix]
S3_GOLD_BUCKET=green-ai-pf-gold-[suffix]
FERNET_KEY=tu_airflow_fernet_key
```

### 4. Orquestación con Airflow

Una vez que la instancia de Airflow esté operativa (ver `airflow_public_ip` en los outputs):

1. Acceda a `http://<airflow_public_ip>:8080` (User/Pass por defecto o definidos en config).
2. **Conexiones:** Configure en la UI de Airflow:
   * `aws_default`: Tipo Amazon Web Services.
   * `spark_ssh_conn`: Tipo SSH (apuntando a la IP de la instancia Spark).
3. **Activación:** Localice el DAG `ingesta_full_pipeline_v3_gold` y actívelo (Toggle ON).
4. **Ejecución:** Dispare el trigger manual para iniciar el proceso E2E.

---

## 📊 Monitoreo y Observabilidad

La plataforma incluye una **Control Tower** (Dashboard en Streamlit) integrada en la infraestructura.

* **Acceso:** `http://100.49.115.216:8501` (o la IP asignada a la instancia `control_tower`).
* **Funcionalidad:** Permite el seguimiento del Pipeline Tracker en tiempo real y la visualización de los KPIs de la capa Gold.

---

## 🛡️ Idempotencia y Resiliencia

Este proyecto ha sido diseñado para superar el  **"Cold Start"** . Gracias al uso de Terraform y la lógica de `overwrite` en Spark:

* Si se destruye la infraestructura con `terraform destroy`, el sistema puede reconstruirse íntegramente en minutos.
* El pipeline es  **idempotente** : su ejecución múltiple no genera duplicados ni inconsistencias en la capa Gold, garantizando la integridad referencial en todo momento.

---

## 👥 Equipo de Desarrollo (Dept. 02)

* **Alejandro Herrera Soria:** Data Engineering, IaC & Governance.
* **Eduardo Cárdenas:** Product Architecture & DevOps.
* **Adrián Velázquez:** Analytics & Visualization.
* **José Frias:** Data Documentation & Quality.
* **Nuri Naranjo:** Business Logic.
