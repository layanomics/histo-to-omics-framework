import argparse
import subprocess
from pathlib import Path
import time
import yaml


def _ts():
    return time.strftime("%Y%m%d_%H%M%S")


def _load_cfg(p: Path) -> dict:
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def _run(cmd, log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    logs_dir = Path(cfg["paths"]["out_logs"])
    log = logs_dir / f"07b_qc_expr_matrix_{_ts()}.log"

    cmd = [
        "python",
        "scripts/shared/qc_expr_matrix.py",
        "--config",
        args.config,
    ]
    _run(cmd, log)
    print(f"[OK] Expression matrix QC finished. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
