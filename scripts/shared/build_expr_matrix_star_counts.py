# scripts/shared/build_expr_matrix_star_counts.py
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _fmt_elapsed(sec: float) -> str:
    sec = int(sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _iter_star_tsvs(download_dir: Path) -> List[Path]:
    # GDC layout: <download_dir>/<file_uuid>/<something>.tsv
    return sorted(download_dir.rglob("*.tsv"))


def _load_manifest_ids(manifest_tsv: Path) -> set:
    df = pd.read_csv(manifest_tsv, sep="\t", dtype=str)
    # standard manifest columns: id, filename
    if "id" not in df.columns:
        raise ValueError(f"Manifest missing column 'id': {manifest_tsv}")
    return set(df["id"].astype(str).tolist())


def _map_file_uuid_to_case_id(meta_tsv: Path) -> Dict[str, str]:
    df = pd.read_csv(meta_tsv, sep="\t", dtype=str)
    # expected: file_id, case_id
    if "file_id" not in df.columns or "case_id" not in df.columns:
        raise ValueError(
            f"Metadata TSV must contain 'file_id' and 'case_id'. "
            f"Got columns={list(df.columns)} in {meta_tsv}"
        )
    m = {}
    for _, r in df.iterrows():
        m[str(r["file_id"])] = str(r["case_id"])
    return m


def _read_star_counts_tsv(path: Path, preferred_cols: List[str]) -> pd.DataFrame:
    # STAR/GDC augmented files may start with comment lines like "# gene-model: ..."
    # comment="#" safely skips those lines.
    df = pd.read_csv(path, sep="\t", comment="#", dtype=str)

    # Sometimes after comment-skipping, pandas might read a single header-like line wrong.
    if df.shape[1] < 2:
        raise ValueError(
            f"Could not parse STAR counts table in {path.name}. "
            f"Columns={list(df.columns)}"
        )

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Identify gene id column (usually first)
    gene_col = df.columns[0]

    # Convert numeric columns
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Pick counts column
    chosen: Optional[str] = None
    for c in preferred_cols:
        if c in df.columns:
            chosen = c
            break
    if chosen is None:
        # fallback: first numeric column after gene_col
        numeric_cols = [c for c in df.columns[1:] if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            raise ValueError(f"No numeric count columns found in {path.name}. Columns={list(df.columns)}")
        chosen = numeric_cols[0]

    out = df[[gene_col, chosen]].copy()
    out = out.rename(columns={gene_col: "gene_id", chosen: "counts"})

    # Drop STAR summary rows (common patterns)
    out = out[~out["gene_id"].astype(str).str.startswith("__")]
    out = out[~out["gene_id"].astype(str).str.startswith("N_")]

    out["gene_id"] = out["gene_id"].astype(str)
    out = out.dropna(subset=["counts"])
    out["counts"] = out["counts"].astype("int64")

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))

    # Required config keys
    expr = cfg.get("expression", {})
    required = [
        "rnaseq_download_dir",
        "rnaseq_manifest_one_per_case",
        "rnaseq_metadata_tsv",
        "out_counts_csv",
        "out_samples_csv",
    ]
    missing = [k for k in required if k not in expr]
    if missing:
        raise ValueError(
            f"Missing keys under config `expression:`: {', '.join(missing)}\n"
            f"Add them to {args.config}"
        )

    download_dir = Path(expr["rnaseq_download_dir"])
    manifest_tsv = Path(expr["rnaseq_manifest_one_per_case"])
    meta_tsv = Path(expr["rnaseq_metadata_tsv"])
    out_counts_csv = Path(expr["out_counts_csv"])
    out_samples_csv = Path(expr["out_samples_csv"])

    preferred_cols = expr.get("counts_column_preference", ["unstranded"])
    progress_every = int(expr.get("progress_every_n_files", 25))

    if not download_dir.exists():
        raise FileNotFoundError(f"RNA download dir not found: {download_dir}")
    if not manifest_tsv.exists():
        raise FileNotFoundError(f"Manifest TSV not found: {manifest_tsv}")
    if not meta_tsv.exists():
        raise FileNotFoundError(f"RNA metadata TSV not found: {meta_tsv}")

    manifest_ids = _load_manifest_ids(manifest_tsv)
    meta_by_file = _map_file_uuid_to_case_id(meta_tsv)

    tsvs = _iter_star_tsvs(download_dir)
    if not tsvs:
        raise RuntimeError(f"No .tsv files found under {download_dir}")

    # Keep only TSVs whose parent folder is the GDC file UUID and in manifest
    selected: List[Path] = []
    for p in tsvs:
        file_uuid = p.parent.name
        if file_uuid in manifest_ids:
            selected.append(p)

    if not selected:
        raise RuntimeError(
            f"No STAR-count TSVs matched manifest IDs.\n"
            f"download_dir={download_dir}\nmanifest={manifest_tsv}"
        )

    selected = sorted(selected)
    n = len(selected)

    print(f"[INFO] Building expression matrix from {n} STAR-counts files", flush=True)
    print(f"[INFO] Download dir: {download_dir}", flush=True)
    print(f"[INFO] Counts column preference: {preferred_cols}", flush=True)

    t0 = time.time()

    # Collect per-sample series
    series_list: List[pd.Series] = []
    sample_infos: List[dict] = []

    last_uuid = ""
    for i, tsv_path in enumerate(selected, start=1):
        file_uuid = tsv_path.parent.name
        last_uuid = file_uuid

        case_id = meta_by_file.get(file_uuid, "")
        if not case_id:
            # still proceed but mark unknown
            case_id = f"UNKNOWN_CASE__{file_uuid}"

        counts_df = _read_star_counts_tsv(tsv_path, preferred_cols=preferred_cols)

        s = counts_df.set_index("gene_id")["counts"]
        s.name = case_id  # columns become case_ids
        series_list.append(s)

        sample_infos.append(
            {
                "case_id": case_id,
                "file_uuid": file_uuid,
                "tsv_path": str(tsv_path),
            }
        )

        if (i == 1) or (i % progress_every == 0) or (i == n):
            elapsed = _fmt_elapsed(time.time() - t0)
            pct = (i / n) * 100.0
            print(
                f"PROGRESS {i}/{n} ({pct:5.1f}%) | elapsed {elapsed} | last={file_uuid}",
                flush=True,
            )

    # Build matrix (gene_id x case_id)
    mat = pd.concat(series_list, axis=1).fillna(0).astype("int64")

    _ensure_parent(out_counts_csv)
    _ensure_parent(out_samples_csv)

    mat.to_csv(out_counts_csv, index=True)
    pd.DataFrame(sample_infos).to_csv(out_samples_csv, index=False)

    print(f"[OK] Wrote counts matrix: {out_counts_csv} (genes={mat.shape[0]} samples={mat.shape[1]})", flush=True)
    print(f"[OK] Wrote sample sheet:  {out_samples_csv} (rows={len(sample_infos)})", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
