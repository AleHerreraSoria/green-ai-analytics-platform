"""Ejecuta export + train del pipeline ML (mismo proceso Python; útil desde Streamlit)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_script(rel_path: str) -> tuple[int, str]:
    script = _REPO_ROOT / rel_path
    if not script.is_file():
        return 1, f"No existe el script: {script}\n"
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=None,
    )
    parts: list[str] = []
    if proc.stdout:
        parts.append(proc.stdout)
    if proc.stderr:
        parts.append(proc.stderr)
    out = "\n".join(parts).strip()
    if not out:
        out = "(sin salida)"
    return proc.returncode, out


def refresh_ml_artifacts() -> tuple[bool, str]:
    """
    Exporta metadatos desde Airflow y reentrena el clasificador.
    Devuelve (éxito, log combinado para mostrar al usuario).
    """
    blocks: list[str] = []

    code, out = _run_script("ML/export_airflow_metadata.py")
    blocks.append("=== python ML/export_airflow_metadata.py ===\n" + out)
    if code != 0:
        return False, "\n\n".join(blocks)

    code, out = _run_script("ML/train_pipeline_ml.py")
    blocks.append("=== python ML/train_pipeline_ml.py ===\n" + out)
    if code != 0:
        return False, "\n\n".join(blocks)

    return True, "\n\n".join(blocks)
