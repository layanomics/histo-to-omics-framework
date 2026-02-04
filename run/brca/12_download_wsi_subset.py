# run/brca/12_download_wsi_subset.py
import argparse
import sys
from datetime import datetime
from pathlib import Path

import yaml
import pandas as pd
import subprocess


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _ts(cfg: dict) -> str:
    fmt = (cfg.get("logging", {}) or {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    return datetime.now().strftime(fmt)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    ap.add_argument("--per_class", type=int, default=10)
    ap.add_argument("--classes", default="LumA,LumB,Basal,Her2")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    paths = cfg["paths"]
    download = cfg.get("download", {}) or {}

    labels_csv = Path(paths["out_tables"]) / "brca_wsi_pam50_slide_labels.csv"
    wsi_meta = Path(paths["meta_dir"]) / "brca_wsi_metadata.tsv"

    manifest_subset = Path(paths["meta_dir"]) / "brca_wsi_manifest_subset.tsv"
    selection_out = Path(paths["out_tables"]) / "brca_wsi_subset_selection.csv"
    out_dir = Path(paths["raw_dir"]) / "wsi_subset"

    out_logs = Path(paths["out_logs"])
    out_logs.mkdir(parents=True, exist_ok=True)
    gdc_log_dir = out_logs / "gdc_download"
    gdc_log_dir.mkdir(parents=True, exist_ok=True)

    runner_log = out_logs / f"12_download_wsi_subset_{_ts(cfg)}.log"

    gdc_client = download.get("gdc_client", "gdc-client")
    n_conn = int(download.get("threads", 8))
    verify_after = bool(download.get("verify_after", True))
    fail_on_verify = bool(download.get("fail_on_verify", True))
    token_file = download.get("token_file")  # optional

    if not labels_csv.exists():
        raise FileNotFoundError(f"Labels CSV not found: {labels_csv}")
    if not wsi_meta.exists():
        raise FileNotFoundError(f"WSI metadata not found: {wsi_meta}")

    labels = pd.read_csv(labels_csv, dtype=str)
    meta = pd.read_csv(wsi_meta, sep="\t", dtype=str)

    # expected columns (from your headers)
    # labels: wsi_file_id, wsi_filename, case_id, submitter_id, pam50_subtype
    # meta  : file_id, file_name, case_id, submitter_id
    if "wsi_file_id" not in labels.columns or "pam50_subtype" not in labels.columns:
        raise ValueError(f"Unexpected labels columns. Found: {labels.columns.tolist()}")
    if "wsi_filename" not in labels.columns:
        raise ValueError("Expected 'wsi_filename' in labels CSV for manifest filename column.")
    if "file_id" not in meta.columns:
        raise ValueError(f"Unexpected wsi metadata columns. Found: {meta.columns.tolist()}")

    classes = [c.strip() for c in args.classes.split(",") if c.strip()]
    per_class = int(args.per_class)

    # sample a balanced subset from labels
    picked = []
    for cls in classes:
        pool = labels[labels["pam50_subtype"] == cls].dropna(subset=["wsi_file_id"])
        if pool.empty:
            print(f"[WARN] No WSIs for class: {cls}")
            continue
        n_take = min(per_class, pool.shape[0])
        if n_take < per_class:
            print(f"[WARN] {cls}: requested {per_class}, available {pool.shape[0]}, taking {n_take}")
        picked.append(pool.sample(n=n_take, random_state=args.seed))

    if not picked:
        raise RuntimeError("No subset selected. Check class names and labels.")

    picked = pd.concat(picked, axis=0).copy()

    # Join via UUID to guarantee mapping consistency
    merged = picked.merge(meta[["file_id"]], left_on="wsi_file_id", right_on="file_id", how="inner")

    # Save selection table for reporting
    selection_out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(selection_out, index=False)

    # Build subset manifest (REQUIRED by scripts/gdc/gdc_download.py): id<TAB>filename
    manifest_df = (
        merged[["file_id", "wsi_filename"]]
        .dropna()
        .drop_duplicates(subset=["file_id"])
        .rename(columns={"file_id": "id", "wsi_filename": "filename"})
    )
    manifest_subset.parent.mkdir(parents=True, exist_ok=True)
    manifest_df.to_csv(manifest_subset, sep="\t", index=False)

    total = int(manifest_df.shape[0])

    cmd = [
        sys.executable,
        "scripts/gdc/gdc_download.py",
        "--manifest", str(manifest_subset),
        "--out_dir", str(out_dir),
        "--log_dir", str(gdc_log_dir),
        "--threads", str(n_conn),
        "--gdc_client", str(gdc_client),
    ]
    if token_file:
        cmd += ["--token_file", str(token_file)]
    if verify_after:
        cmd.append("--verify_after")
    if fail_on_verify:
        cmd.append("--fail_on_verify")

    if args.dry_run:
        print("[DRY RUN] WSI download (SUBSET) preview")
        print(f"  Labels CSV: {labels_csv}")
        print(f"  WSI metadata: {wsi_meta}")
        print(f"  Subset selection out: {selection_out}")
        print(f"  Subset manifest: {manifest_subset}")
        print(f"  Classes: {classes}")
        print(f"  per_class: {per_class}")
        print(f"  Files to download: {total}")
        print(f"  Output directory: {out_dir}")
        print(f"  Connections (-n): {n_conn}")
        print(f"  GDC client: {gdc_client}")
        print(f"  verify_after: {verify_after}")
        print(f"  fail_on_verify: {fail_on_verify}")
        if token_file:
            print(f"  token_file: {token_file}")
        print("\n  First 10 selected rows:")
        print(merged.head(10))
        print("\n[DRY RUN] No files were downloaded.")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)

    with runner_log.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n")
        lf.write(" ".join(cmd) + "\n\n")
        lf.write(f"GDC_DOWNLOAD_LOG_DIR: {gdc_log_dir}\n")
        lf.write(f"SELECTION_TABLE: {selection_out}\n")
        lf.write(f"SUBSET_MANIFEST: {manifest_subset}\n")

    try:
        subprocess.run(cmd, check=True)
        print(f"[OK] Download finished. Runner log:\n  {runner_log}")
        print(f"[OK] Subset selection:\n  {selection_out}")
        print(f"[OK] Subset manifest:\n  {manifest_subset}")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Download failed (exit={e.returncode}). Runner log:\n  {runner_log}")
        print(f"[INFO] Check gdc logs in:\n  {gdc_log_dir}")
        return e.returncode


if __name__ == "__main__":
    raise SystemExit(main())
