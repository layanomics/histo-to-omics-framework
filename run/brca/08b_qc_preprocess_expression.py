import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import yaml


def _ts(fmt: str) -> str:
    return datetime.now().strftime(fmt)


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _run(cmd: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Runner: Step 08b QC preprocess expression")
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    ts = _ts(cfg.get("logging", {}).get("timestamp_format", "%Y%m%d_%H%M%S"))
    out_logs = Path(cfg["paths"]["out_logs"])
    log = out_logs / f"08b_qc_preprocess_expression_{ts}.log"

    print("[RUNNING] Step 08b - QC preprocess expression")
    print(f"[INFO] Log: {log.as_posix()}")

    cmd = [
        str(Path.cwd() / "C:\\Users\\layan\\miniconda3\\envs\\gdc\\python.exe")
        if False else "python",
        "scripts/shared/qc_preprocess_expression.py",
        "--config",
        args.config,
    ]

    try:
        _run(cmd, log)
    except subprocess.CalledProcessError:
        print(f"[ERROR] Step 08b failed. Check log: {log.as_posix()}")
        return 1

    print("[OK] QC finished.")
    print(f"[OK] Log: {log.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
