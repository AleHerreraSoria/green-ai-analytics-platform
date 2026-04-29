import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    SparkSession para testing local.

    - local[1]: evita consumo alto
    - session scope: más rápido
    - stop(): evita procesos colgados en Windows
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )

    yield spark

    # cierre limpio
    spark.stop()