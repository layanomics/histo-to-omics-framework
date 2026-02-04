#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(optparse)
  library(genefu)
  library(AnnotationDbi)
  library(org.Hs.eg.db)
})

opt_list <- list(
  make_option(c("--counts_preprocessed_csv"), type="character", help="Gene x sample matrix CSV. First column is gene_id."),
  make_option(c("--samples_csv"), type="character", help="Sample sheet CSV (kept for provenance)."),
  make_option(c("--out_raw_calls_csv"), type="character", help="Output CSV for PAM50 raw calls."),
  make_option(c("--gene_id_col"), type="character", default="gene_id", help="Gene id column name in counts CSV [default %default]."),
  make_option(c("--quiet"), action="store_true", default=FALSE, help="Reduce console output.")
)
opt <- parse_args(OptionParser(option_list=opt_list))

if (is.null(opt$counts_preprocessed_csv) || is.null(opt$samples_csv) || is.null(opt$out_raw_calls_csv)) {
  stop("Missing required args: --counts_preprocessed_csv --samples_csv --out_raw_calls_csv")
}

msg <- function(...) { if (!opt$quiet) cat(sprintf(...), "\n") }

msg("[INFO] Reading matrix: %s", opt$counts_preprocessed_csv)
df <- read.csv(opt$counts_preprocessed_csv, check.names=FALSE, stringsAsFactors=FALSE)

if (!(opt$gene_id_col %in% colnames(df))) {
  stop(sprintf("gene_id_col '%s' not found in counts CSV. Columns=%s",
               opt$gene_id_col, paste(colnames(df), collapse=", ")))
}

gene_ids <- df[[opt$gene_id_col]]
df[[opt$gene_id_col]] <- NULL

# numeric matrix
mat <- as.matrix(df)
storage.mode(mat) <- "numeric"
rownames(mat) <- gene_ids

msg("[INFO] Matrix dims: genes=%d samples=%d", nrow(mat), ncol(mat))

# Detect gene-id type and map to SYMBOL if needed
is_ensembl <- all(grepl("^ENSG", rownames(mat)))
if (is_ensembl) {
  msg("[INFO] Detected Ensembl gene IDs. Mapping ENSEMBL -> SYMBOL using org.Hs.eg.db")

  ens <- rownames(mat)
  ens_clean <- sub("\\..*$", "", ens)  # strip version ENSG...<.v>
  rownames(mat) <- ens_clean

  mapped <- AnnotationDbi::select(
    org.Hs.eg.db,
    keys = unique(ens_clean),
    keytype = "ENSEMBL",
    columns = c("SYMBOL")
  )

  mapped <- mapped[!is.na(mapped$SYMBOL) & mapped$SYMBOL != "", ]
  # build lookup ENS -> SYMBOL (take first SYMBOL per ENSEMBL deterministically)
  mapped <- mapped[order(mapped$ENSEMBL, mapped$SYMBOL), ]
  mapped <- mapped[!duplicated(mapped$ENSEMBL), ]

  idx <- match(rownames(mat), mapped$ENSEMBL)
  symbols <- mapped$SYMBOL[idx]

  keep <- !is.na(symbols) & symbols != ""
  mat <- mat[keep, , drop=FALSE]
  symbols <- symbols[keep]

  # Aggregate duplicates by SYMBOL (sum counts)
  msg("[INFO] Aggregating duplicates after mapping (sum by SYMBOL)")
  mat_sum <- rowsum(mat, group = symbols, reorder = TRUE)
  mat <- mat_sum
  rm(mat_sum)

  msg("[INFO] After mapping: genes=%d samples=%d", nrow(mat), ncol(mat))
} else {
  msg("[INFO] Gene IDs do not look like Ensembl. Assuming they are SYMBOL already.")
}

# genefu expects samples x genes
dat <- t(mat)
msg("[INFO] Transposed dims: samples=%d genes=%d", nrow(dat), ncol(dat))

annot <- data.frame(GENE = colnames(dat), stringsAsFactors=FALSE)

msg("[INFO] Running genefu::molecular.subtyping (pam50)")
res <- genefu::molecular.subtyping(
  sbt.model = "pam50",
  data = dat,
  annot = annot
)

# Try to standardize output
out <- NULL
if (is.data.frame(res)) {
  out <- res
} else if (is.list(res) && !is.null(res$subtype)) {
  out <- data.frame(sample_id = rownames(dat), pam50_subtype = res$subtype, stringsAsFactors=FALSE)
} else if (is.list(res) && !is.null(res$subtype.pred)) {
  out <- data.frame(sample_id = rownames(dat), pam50_subtype = res$subtype.pred, stringsAsFactors=FALSE)
} else {
  # fallback: print structure into error for debugging
  stop("Unexpected result format from molecular.subtyping. Inspect object structure in logs.")
}

# Ensure sample_id exists
if (!("sample_id" %in% colnames(out))) {
  if (!is.null(rownames(out)) && length(rownames(out)) == nrow(out)) {
    out$sample_id <- rownames(out)
  }
}

dir.create(dirname(opt$out_raw_calls_csv), recursive=TRUE, showWarnings=FALSE)
write.csv(out, opt$out_raw_calls_csv, row.names=FALSE)
msg("[OK] Wrote PAM50 raw calls: %s (rows=%d cols=%d)", opt$out_raw_calls_csv, nrow(out), ncol(out))
