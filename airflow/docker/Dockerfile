FROM apache/airflow:2.8.1

USER root
# Java para utilidades que puedan requerir JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Dependencias de la instancia Airflow (orquestación + ingesta liviana).
COPY requirements/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt