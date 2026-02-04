# scripts/gdc/gdc_build_manifests.py
import argparse
from pathlib import Path
import requests
from collections import Counter

GDC_FILES_ENDPOINT = "https://api.gdc.cancer.gov/files"


def _post_files(filters: dict, fields: list[str], size: int = 50000) -> list[dict]:
    payload = {
        "filters": filters,
        "fields": ",".join(fields),
        "format": "JSON",
        "size": size,
    }
    r = requests.post(GDC_FILES_ENDPOINT, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["data"]["hits"]


def _write_manifest(hits: list[dict], out_manifest: Path, n: int):
    out_manifest.parent.mkdir(parents=True, exist_ok=True)
    with out_manifest.open("w", encoding="utf-8") as f:
        f.write("id\tfilename\n")
        for h in hits[:n]:
            f.write(f"{h['file_id']}\t{h['file_name']}\n")


def _write_metadata_tsv(hits: list[dict], out_meta: Path, kind: str, n: int):
    """
    Minimal, pairing-ready metadata:
      file_id, file_name, case_id, submitter_id (+ rnaseq extras)
    """
    out_meta.parent.mkdir(parents=True, exist_ok=True)

    header = ["file_id", "file_name", "case_id", "submitter_id"]
    if kind == "rnaseq":
        header += ["workflow_type", "sample_type"]

    with out_meta.open("w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")

        for h in hits[:n]:
            file_id = h.get("file_id", "")
            file_name = h.get("file_name", "")

            case_id = ""
            submitter_id = ""
            sample_type = ""
            if h.get("cases"):
                c0 = h["cases"][0]
                case_id = c0.get("case_id", "")
                submitter_id = c0.get("submitter_id", "")
                if kind == "rnaseq" and c0.get("samples"):
                    sample_type = c0["samples"][0].get("sample_type", "")

            workflow_type = ""
            if kind == "rnaseq":
                workflow_type = (h.get("analysis") or {}).get("workflow_type", "")

            row = [file_id, file_name, case_id, submitter_id]
            if kind == "rnaseq":
                row += [workflow_type, sample_type]

            f.write("\t".join(str(x) for x in row) + "\n")


def main():
    ap = argparse.ArgumentParser(
        description="Build GDC manifests + pairing-ready metadata (shared across cancers)."
    )
    ap.add_argument("--project", required=True, help="e.g., TCGA-BRCA")
    ap.add_argument("--data_kind", choices=["rnaseq", "wsi"], required=True)

    # Full cohort support:
    ap.add_argument(
        "--n",
        default="all",
        help="How many files to write. Use integer (e.g., 200) or 'all' for full cohort.",
    )

    ap.add_argument("--out_manifest", required=True)
    ap.add_argument("--out_meta", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    # ---- Filters (Phase-1 defaults; project is parameterized) ----
    if args.data_kind == "rnaseq":
        # RNA-seq STAR - Counts, Primary Tumor (contract v1)
        content = [
            {"op": "in", "content": {"field": "cases.project.project_id", "value": [args.project]}},
            {"op": "in", "content": {"field": "files.data_category", "value": ["Transcriptome Profiling"]}},
            {"op": "in", "content": {"field": "files.data_type", "value": ["Gene Expression Quantification"]}},
            {"op": "in", "content": {"field": "files.experimental_strategy", "value": ["RNA-Seq"]}},
            {"op": "in", "content": {"field": "files.analysis.workflow_type", "value": ["STAR - Counts"]}},
            {"op": "in", "content": {"field": "cases.samples.sample_type", "value": ["Primary Tumor"]}},
            {"op": "in", "content": {"field": "files.access", "value": ["open"]}},
        ]
        filters = {"op": "and", "content": content}

        fields = [
            "file_id",
            "file_name",
            "analysis.workflow_type",
            "cases.case_id",
            "cases.submitter_id",
            "cases.samples.sample_type",
        ]

    else:
        # WSI slide images (SVS)
        content = [
            {"op": "in", "content": {"field": "cases.project.project_id", "value": [args.project]}},
            {"op": "in", "content": {"field": "files.data_category", "value": ["Biospecimen"]}},
            {"op": "in", "content": {"field": "files.data_type", "value": ["Slide Image"]}},
            {"op": "in", "content": {"field": "files.data_format", "value": ["SVS"]}},
            {"op": "in", "content": {"field": "files.access", "value": ["open"]}},
        ]
        filters = {"op": "and", "content": content}

        # Minimal fields we actually write:
        fields = [
            "file_id",
            "file_name",
            "cases.case_id",
            "cases.submitter_id",
        ]

    # ---- Query ----
    hits = _post_files(filters=filters, fields=fields, size=50000)
    if len(hits) == 0:
        raise RuntimeError("No files returned from GDC for the specified kind/filters.")

    # ---- Resolve how many to write ----
    if str(args.n).lower() == "all":
        n_to_write = len(hits)
    else:
        n_to_write = min(int(args.n), len(hits))

    # ---- Debug summaries ----
    if args.debug:
        print("[DEBUG] Returned hits:", len(hits))
        if args.data_kind == "rnaseq":
            wf = [h.get("analysis", {}).get("workflow_type", None) for h in hits]
            wf = [x for x in wf if x]
            print("[DEBUG] Top workflow_type counts:")
            for k, v in Counter(wf).most_common(10):
                print(f"  {v:>5}  {k}")

            st = []
            for h in hits:
                for c in h.get("cases", []):
                    for s in c.get("samples", []):
                        st.append(s.get("sample_type"))
            st = [x for x in st if x]
            print("[DEBUG] Top sample_type counts:")
            for k, v in Counter(st).most_common(10):
                print(f"  {v:>5}  {k}")

    # ---- Write outputs ----
    out_manifest = Path(args.out_manifest)
    out_meta = Path(args.out_meta)

    _write_manifest(hits, out_manifest, n=n_to_write)
    _write_metadata_tsv(hits, out_meta, kind=args.data_kind, n=n_to_write)

    print(f"[OK] {args.data_kind} -> wrote manifest: {out_manifest}")
    print(f"[OK] {args.data_kind} -> wrote metadata:  {out_meta}")
    print(f"[INFO] Total hits in GDC = {len(hits)} ; wrote n = {n_to_write}")


if __name__ == "__main__":
    main()
