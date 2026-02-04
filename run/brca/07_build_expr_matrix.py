# run/brca/07_build_expr_matrix.py
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _run_with_filtered_live_output(cmd, log_path: Path) -> int:
    """
    - Writes ALL subprocess output to log file.
    - Shows only progress + key lines in terminal.
    - Progress lines are expected to start with: 'PROGRESS '
    """
    _ensure_dir(log_path.parent)

    with log_path.open("w", encoding="utf-8", newline="\n") as lf:
        lf.write("COMMAND:\n")
        lf.write(" ".join(map(str, cmd)) + "\n\n")
        lf.flush()

        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        last_progress_printed = False

        assert p.stdout is not None
        for line in p.stdout:
            lf.write(line)
            lf.flush()

            s = line.rstrip("\n")

            # Show start/end info and progress only
            if s.startswith("PROGRESS "):
                # overwrite one line in terminal
                print("\r" + s, end="", flush=True)
                last_progress_printed = True
            elif s.startswith("[INFO]"):
                # print as normal line
                if last_progress_printed:
                    print("")  # newline after progress line
                    last_progress_printed = False
                print(s, flush=True)
            elif s.startswith("[OK]") or s.startswith("[ERROR]"):
                if last_progress_printed:
                    print("")  # newline after progress line
                    last_progress_printed = False
                print(s, flush=True)

        rc = p.wait()

        if last_progress_printed:
            print("")  # ensure terminal ends with newline

        return rc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    out_logs = Path(cfg["paths"]["out_logs"])
    _ensure_dir(out_logs)

    log = out_logs / f"07_build_expr_matrix_{_ts()}.log"

    cmd = [
        sys.executable,
        "scripts/shared/build_expr_matrix_star_counts.py",
        "--config",
        args.config,
    ]

    rc = _run_with_filtered_live_output(cmd, log)
    if rc != 0:
        raise SystemExit(
            f"[ERROR] Step 07 failed (rc={rc}). See log:\n  {log}"
        )

    print(f"[OK] Expression matrix finished. Log: {log}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
