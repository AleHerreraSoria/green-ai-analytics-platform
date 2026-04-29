"""
CLI: score del último run. La lógica vive en `ML.scoring`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ML.scoring import score_latest_run_cli_print


def main() -> None:
    parser = argparse.ArgumentParser(description="Score del último run vs modelo entrenado.")
    parser.add_argument(
        "--history",
        type=Path,
        default=None,
        help="Parquet histórico (por defecto ML/data/ o ML_HISTORY_PARQUET).",
    )
    parser.add_argument("--model", type=Path, default=None)
    parser.add_argument("--metrics", type=Path, default=None)
    parser.add_argument("--ti-page-size", type=int, default=200)
    args = parser.parse_args()

    raise SystemExit(
        score_latest_run_cli_print(
            history_parquet=args.history,
            model_path=args.model,
            metrics_path=args.metrics,
            ti_page_size=args.ti_page_size,
        )
    )


if __name__ == "__main__":
    main()
