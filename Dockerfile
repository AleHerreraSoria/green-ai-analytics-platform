# Dockerfile
FROM apache/airflow:2.8.1

USER root
# Instalamos Java de forma permanente (necesario para Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Instalamos las librerías de Python para que ya estén listas al arrancar
RUN pip install --no-cache-dir pandas pendulum requests pyspark