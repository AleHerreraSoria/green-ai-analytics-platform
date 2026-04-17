# 🏗️ Guía de Infraestructura y Acceso - Green AI

Este documento detalla la configuración técnica de los servidores, el almacenamiento y los protocolos de acceso para el equipo de Data Engineering de **Green AI Analytics Platform**.

---

## 📍 Inventario de Recursos (Nodos EC2)

Hemos desplegado una arquitectura de nodos independientes para separar las cargas de trabajo de orquestación y procesamiento pesado.

| Instancia | IP Pública | Rol Principal | Usuario | Tipo de Instancia |
| :--- | :--- | :--- | :--- | :--- |
| **Airflow-Orchestrator** | `23.23.191.38` | Orquestación (Docker/DAGs) | `ubuntu` | m7i-flex.large |
| **Spark-Processor** | `52.70.198.116` | Procesamiento (Spark/PySpark) | `ubuntu` | m7i-flex.large |

---

## 🔑 Acceso Seguro (SSH)

Ambos servidores utilizan la llave privada `green-ai-final-key.pem`. 

> [!CAUTION]
> **NO SUBIR ESTA LLAVE AL REPOSITORIO.** Asegúrate de que `*.pem` esté incluido en el archivo `.gitignore` o en una carpeta "fuera" del repositorio.

### Configuración de permisos (Requerido)

El cliente SSH requiere que la llave tenga permisos restrictivos para ser aceptada.

#### En Windows (PowerShell):
Ejecuta estos comandos en la carpeta donde se encuentra tu archivo `.pem`:
```powershell
icacls.exe "green-ai-final-key.pem" /inheritance:r
icacls.exe "green-ai-final-key.pem" /grant:r "$($env:USERNAME):(R)"
En Linux / macOS / WSL:
Bash
chmod 400 green-ai-final-key.pem
Comando de entrada:
Bash
ssh -i "green-ai-final-key.pem" ubuntu@<IP_DEL_SERVIDOR>

---

🗄️ Almacenamiento (Amazon S3)
Contamos con tres buckets principales que siguen la Arquitectura Medallion para garantizar la trazabilidad y calidad del dato:

Bronze Layer (Raw): green-ai-pf-bronze-a0e96d06

Uso: Ingesta de datos crudos (JSON/CSV) tal cual llegan de las APIs.

Silver Layer (Cleansed): green-ai-pf-silver-a0e96d06

Uso: Datos limpios, tipados y transformados a formato Parquet.

Gold Layer (Curated): green-ai-pf-gold-a0e96d06

Uso: Modelo dimensional final (Hechos y Dimensiones) para consumo analítico.

---

📁 Rutas del Proyecto en Servidores
Para mantener la consistencia entre servidores, se debe respetar la siguiente estructura de directorios:

Raíz del Repositorio: /home/ubuntu/green-ai-platform/

Datos Locales (Temporales): /home/ubuntu/green-ai-platform/data/

Scripts de Procesamiento: /home/ubuntu/green-ai-platform/processor/

Logs de Sistema: /home/ubuntu/green-ai-platform/logs/