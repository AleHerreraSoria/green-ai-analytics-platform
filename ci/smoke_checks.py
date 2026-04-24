from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT / ".env.example"
COMPOSE = ROOT / "airflow" / "docker" / "docker-compose.yml"

REQUIRED_ENV_VARS = {
    "APP_ENV",
    "AIRFLOW__CORE__FERNET_KEY",
    "S3_BRONZE_BUCKET",
    "S3_SILVER_BUCKET",
    "S3_GOLD_BUCKET",
    "SPARK_SSH_CONN_ID",
    "AIRFLOW_AWS_CONN_ID",
}


def fail(message: str) -> None:
    print(f"SMOKE_CHECK_FAILED: {message}")
    sys.exit(1)


def main() -> None:
    if not ENV_EXAMPLE.exists():
        fail(".env.example not found")

    env_content = ENV_EXAMPLE.read_text(encoding="utf-8")
    found = {
        line.split("=", 1)[0].strip()
        for line in env_content.splitlines()
        if line.strip() and not line.strip().startswith("#") and "=" in line
    }
    missing = sorted(REQUIRED_ENV_VARS - found)
    if missing:
        fail(f"Missing required vars in .env.example: {', '.join(missing)}")

    compose_content = COMPOSE.read_text(encoding="utf-8")
    if "redis:latest" in compose_content:
        fail("Floating image tag redis:latest is not allowed")
    if re.search(r"FERNET_KEY:\s*''", compose_content):
        fail("Empty Fernet key default found in docker-compose")

    print("Smoke checks passed")


if __name__ == "__main__":
    main()
