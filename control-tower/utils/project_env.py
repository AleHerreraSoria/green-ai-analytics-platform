"""Carga `.env` en la raíz del repo para que Streamlit vea variables sin exportarlas en el shell."""

from __future__ import annotations

from pathlib import Path

_loaded = False


def ensure_dotenv_loaded() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parent.parent
    # override=True: valores del `.env` del repo sustituyen variables ya heredadas del shell
    # (p. ej. un PIPELINE_DAG_ID viejo exportado en .bashrc); en prod suele no haber `.env` en la imagen.
    load_dotenv(root / ".env", override=True)
