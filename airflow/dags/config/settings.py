import os


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


SCRIPTS_PATH = require_env("AIRFLOW_SCRIPTS_PATH")
DATA_PATH = require_env("AIRFLOW_DATA_PATH")
MLCO2_LOCAL_DIR = f"{DATA_PATH}/Code_Carbon"

S3_BRONZE_BUCKET = require_env("S3_BRONZE_BUCKET")
S3_SILVER_BUCKET = require_env("S3_SILVER_BUCKET")
S3_GOLD_BUCKET = require_env("S3_GOLD_BUCKET")

SPARK_SSH_CONN_ID = require_env("SPARK_SSH_CONN_ID")
SPARK_REPO_PATH = require_env("SPARK_REPO_PATH")
AWS_CONN_ID = require_env("AIRFLOW_AWS_CONN_ID")
