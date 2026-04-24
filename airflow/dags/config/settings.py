import os


def require_env(var_name: str) -> str:
    value = (os.getenv(var_name) or "").strip()
    # Avoid DAG import failures when env vars are missing.
    # Tasks may still fail at runtime if a required value is empty.
    return value


SCRIPTS_PATH = require_env("AIRFLOW_SCRIPTS_PATH")
DATA_PATH = require_env("AIRFLOW_DATA_PATH")
MLCO2_LOCAL_DIR = f"{DATA_PATH}/Code_Carbon"

S3_BRONZE_BUCKET = require_env("S3_BRONZE_BUCKET")
S3_SILVER_BUCKET = require_env("S3_SILVER_BUCKET")
S3_GOLD_BUCKET = require_env("S3_GOLD_BUCKET")
S3_SOURCES_BUCKET = require_env("S3_SOURCES_BUCKET")

SPARK_SSH_CONN_ID = require_env("SPARK_SSH_CONN_ID")
SPARK_REPO_PATH = require_env("SPARK_REPO_PATH")
SPARK_SUBMIT_BIN = require_env("SPARK_SUBMIT_BIN")
# Extra args for remote spark-submit (e.g. --packages io.delta:delta-spark_2.12:2.4.0). May be empty.
SPARK_SUBMIT_EXTRA_ARGS = require_env("SPARK_SUBMIT_EXTRA_ARGS")
AWS_CONN_ID = require_env("AIRFLOW_AWS_CONN_ID")
