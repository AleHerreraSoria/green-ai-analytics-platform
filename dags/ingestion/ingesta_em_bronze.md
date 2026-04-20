# Pipeline de Ingesta: Electricity Maps a S3 (Capa Bronze)

## 1. Descripción General
Este DAG se encarga de la extracción automatizada de datos de intensidad de carbono desde la API de **Electricity Maps**. El proceso es dinámico, permitiendo escalar la cantidad de zonas geográficas monitoreadas simplemente modificando un archivo de configuración CSV.

- **ID del DAG:** `ingesta_em_bronze_SAFE_V4`
- **Frecuencia:** Diaria (`@daily`)
- **Capa de Datos:** Bronze (Raw Data)

## 2. Arquitectura del Proceso

El flujo de datos sigue los siguientes pasos:
1. **Lectura de Configuración:** El DAG lee el archivo `geo_cloud_to_country_and_zones.csv` para identificar las zonas activas.
2. **Generación Dinámica:** Se crea un `PythonOperator` independiente para cada zona encontrada.
3. **Extracción (API):** Cada tarea consulta el endpoint `/carbon-intensity/past-range` para las últimas 24 horas.
4. **Carga (S3):** Los datos se almacenan en formato JSON crudo en el bucket de destino.

## 3. Especificaciones Técnicas

### Optimización de Recursos (Throttling)
Debido a las limitaciones de hardware de la instancia EC2, el DAG implementa controles de concurrencia estrictos:
- `max_active_tasks=3`: Solo se ejecutan 3 zonas en paralelo para evitar saturación de CPU y memoria.
- `max_active_runs=1`: Evita que se solapen ejecuciones de distintos días.
- `catchup=False`: El DAG no intenta recuperar días pasados automáticamente al activarse, previniendo picos de carga innecesarios.

### Stack Tecnológico
- **Orquestador:** Airflow 3.x
- **Lenguaje:** Python 3.13
- **Librerías Clave:** - `pendulum`: Para manejo preciso de zonas horarias y fechas estáticas.
    - `pandas`: Procesamiento del CSV de configuración.
    - `S3Hook`: Integración nativa con AWS.

## 4. Estructura de Destino (S3)
Los archivos se guardan siguiendo el estándar **Hive-style partitioning**, lo que facilita la lectura posterior desde Spark o Athena:

`s3://{bucket_bronze}/electricity_maps/zone={zona}/year={yyyy}/month={mm}/day={dd}/data.json`

## 5. Configuración y Variables
El DAG requiere que las siguientes **Airflow Variables** estén configuradas:
- `em_api_token`: Token de autenticación de la API de Electricity Maps.
- `s3_bronze`: Nombre del bucket de AWS para la capa Bronze.
- **Conexión AWS:** Debe existir una conexión de tipo Amazon Web Services con ID `aws_default`.

## 6. Mantenimiento
### Añadir nuevas zonas
Para añadir o quitar zonas de la ingesta, no es necesario modificar el código del DAG. Simplemente edite el archivo:
`~/green-ai-platform/orchestrator/dags/data/geo_cloud_to_country_and_zones.csv`

### Reinstalar dependencias
En caso de reconstrucción de contenedores, si las tareas fallan por falta de `pandas`, ejecutar:
```bash
sudo docker exec -u airflow -it $(sudo docker ps -qf "name=scheduler") python -m pip install pandas


---
*Confeccionado por: Alejandro Herrera (Data Engineering) PF: Grupo 2 - DEPT02*
*Documentación generada el 20 de abril de 2026.*