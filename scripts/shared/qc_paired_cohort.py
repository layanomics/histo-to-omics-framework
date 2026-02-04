import argparse
import pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="QC summary for paired cohort table.")
    ap.add_argument("--paired_csv", required=True, help="Path to paired cohort CSV")
    ap.add_argument("--out_txt", required=True, help="Where to write QC summary text")
    args = ap.parse_args()

    paired_csv = Path(args.paired_csv)
    out_txt = Path(args.out_txt)
    out_txt.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(paired_csv)

    lines = []
    lines.append(f"FILE: {paired_csv.as_posix()}")
    lines.append(f"SHAPE: {df.shape}")
    lines.append("")
    lines.append("VALUE_COUNTS has_rnaseq x has_wsi:")
    vc = df[["has_rnaseq", "has_wsi"]].value_counts()
    lines.append(vc.to_string())
    lines.append("")
    if "wsi_slide_count" in df.columns:
        lines.append(f"WSI_SLIDE_COUNT min={df.wsi_slide_count.min()} max={df.wsi_slide_count.max()}")
    else:
        lines.append("WSI_SLIDE_COUNT column not found")

    text = "\n".join(lines) + "\n"
    print(text)
    out_txt.write_text(text, encoding="utf-8")
    print(f"[OK] wrote QC summary -> {out_txt}")

if __name__ == "__main__":
    main()
