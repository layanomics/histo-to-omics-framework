# scripts/gdc/gdc_download.py
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Thread, Lock


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_manifest(manifest: Path) -> list[dict]:
    rows: list[dict] = []
    with manifest.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        if header[:2] != ["id", "filename"]:
            raise SystemExit(f"Manifest header must be: id<tab>filename. Got: {header[:5]}")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            rows.append({"id": parts[0], "filename": parts[1]})
    return rows


def _expected_path(out_dir: Path, file_id: str, filename: str) -> Path:
    # gdc-client stores each file under: <out_dir>/<file_id>/<filename>
    return out_dir / file_id / filename


def _verify_download(manifest_rows: list[dict], out_dir: Path, report_csv: Path) -> tuple[int, int, int]:
    """Returns: (ok, missing, empty)"""
    report_csv.parent.mkdir(parents=True, exist_ok=True)

    ok = missing = empty = 0
    with report_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "filename", "expected_path", "status", "size_bytes"])

        for r in manifest_rows:
            p = _expected_path(out_dir, r["id"], r["filename"])
            if not p.exists():
                missing += 1
                w.writerow([r["id"], r["filename"], str(p), "MISSING", ""])
                continue

            size = p.stat().st_size
            if size == 0:
                empty += 1
                w.writerow([r["id"], r["filename"], str(p), "EMPTY", str(size)])
                continue

            ok += 1
            w.writerow([r["id"], r["filename"], str(p), "OK", str(size)])

    return ok, missing, empty


def _count_completed(manifest_rows: list[dict], out_dir: Path) -> int:
    done = 0
    for r in manifest_rows:
        if _expected_path(out_dir, r["id"], r["filename"]).exists():
            done += 1
    return done


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    hh = s // 3600
    mm = (s % 3600) // 60
    ss = s % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def main() -> int:
    ap = argparse.ArgumentParser(description="GDC download wrapper with logging + verification + clean progress line.")
    ap.add_argument("--manifest", required=True, help="TSV with columns: id, filename")
    ap.add_argument("--out_dir", required=True, help="Download directory")
    ap.add_argument("--log_dir", required=True, help="Where to write logs + verification report")
    ap.add_argument("--threads", type=int, default=8, help="Maps to gdc-client -n / --n-processes")
    ap.add_argument("--gdc_client", default="gdc-client", help="gdc-client executable or full path")
    ap.add_argument("--token_file", default=None, help="Optional token file (controlled access)")
    ap.add_argument("--verify_after", action="store_true", help="Verify missing/empty after download")
    ap.add_argument("--fail_on_verify", action="store_true", help="Exit non-zero if verification finds problems")
    ap.add_argument("--progress_every", type=int, default=10, help="Seconds between terminal progress updates")
    args = ap.parse_args()

    manifest = Path(args.manifest)
    out_dir = Path(args.out_dir)
    log_dir = Path(args.log_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    manifest_rows = _read_manifest(manifest)
    total = len(manifest_rows)
    if total == 0:
        raise SystemExit("Manifest has 0 rows. Nothing to download.")

    cmd = [
        args.gdc_client,
        "download",
        "-m",
        str(manifest),
        "-d",
        str(out_dir),
        "-n",
        str(args.threads),
    ]
    if args.token_file:
        cmd.extend(["-t", str(args.token_file)])

    log_file = log_dir / f"gdc_download_{_ts()}.log"

    last_line = {"text": ""}
    last_line_lock = Lock()

    def reader_thread(proc: subprocess.Popen, lf):
        for line in proc.stdout:  # type: ignore[attr-defined]
            lf.write(line)
            lf.flush()
            with last_line_lock:
                last_line["text"] = line.strip()

    with log_file.open("w", encoding="utf-8") as lf:
        lf.write(f"[CMD] {' '.join(cmd)}\n")
        lf.write(f"[MANIFEST] {manifest}\n")
        lf.write(f"[OUT_DIR] {out_dir}\n")
        lf.write(f"[THREADS] {args.threads}\n")
        if args.token_file:
            lf.write(f"[TOKEN_FILE] {args.token_file}\n")
        lf.write("\n")
        lf.flush()

        start = time.time()
        initial_done = _count_completed(manifest_rows, out_dir)

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        t = Thread(target=reader_thread, args=(proc, lf), daemon=True)
        t.start()

        try:
            while True:
                rc = proc.poll()
                done = _count_completed(manifest_rows, out_dir)
                elapsed = _fmt_elapsed(time.time() - start)
                pct = (done / total) * 100.0

                with last_line_lock:
                    tail = last_line["text"]

                tail_short = ""
                if tail and any(k in tail.lower() for k in ["download", "error", "retry", "complete", "saved", "skipping"]):
                    tail_short = f" | last: {tail[:120]}"

                msg = f"\rProgress: {done}/{total} ({pct:5.1f}%) | elapsed {elapsed} | resumed_from {initial_done}{tail_short}"
                sys.stdout.write(msg)
                sys.stdout.flush()

                if rc is not None:
                    break
                time.sleep(max(1, args.progress_every))
        finally:
            sys.stdout.write("\n")
            sys.stdout.flush()

        t.join(timeout=5)

        if proc.returncode != 0:
            lf.write(f"\n[ERROR] gdc-client exited with code {proc.returncode}\n")
            lf.flush()
            return int(proc.returncode)

    if args.verify_after:
        report = log_dir / f"download_verify_{_ts()}.csv"
        ok, missing, empty = _verify_download(manifest_rows, out_dir, report)

        summary = (
            f"[VERIFY] OK={ok} MISSING={missing} EMPTY={empty}\n"
            f"[VERIFY] Report: {report}\n"
        )
        with log_file.open("a", encoding="utf-8") as lf:
            lf.write("\n" + summary)

        print(summary.rstrip())

        if (missing > 0 or empty > 0) and args.fail_on_verify:
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
