# run/brca/06_inspect_star_counts_schema.py
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import yaml


def _ts(cfg) -> str:
    fmt = cfg.get("logging", {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    return datetime.now().strftime(fmt)


def _load(cfg_path: Path) -> dict:
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg_path = Path(args.config)
    cfg = _load(cfg_path)

    out_logs = Path(cfg["paths"]["out_logs"])
    out_logs.mkdir(parents=True, exist_ok=True)
    log = out_logs / f"06_inspect_star_counts_schema_{_ts(cfg)}.log"

    cmd = [
        str(Path("C:/") / "dummy")  # placeholder overwritten below
    ]

    # Use the current python executable for conda safety
    import sys
    py = sys.executable

    cmd = [
        py,
        "scripts/shared/inspect_star_counts_schema.py",
        "--config",
        str(cfg_path),
    ]

    with log.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)

    print(f"[OK] Schema inspection finished. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
