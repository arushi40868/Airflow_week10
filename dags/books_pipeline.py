from __future__ import annotations

import os
from pathlib import Path
import glob
import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import pendulum


# -------- Paths (match docker-compose mounts) --------
DATA_ROOT = Path("/opt/airflow/data")
RAW_DIR = DATA_ROOT / "raw"
STAGE_DIR = DATA_ROOT / "stage"
OUT_DIR = DATA_ROOT / "outputs"

# Postgres target (compose service = dw). Override with env if needed.
DW_URL = os.environ.get(
    "BOOKS_DW_URL",
    "postgresql+psycopg2://airflow:airflow@dw:5432/books_dw",
)

default_args = {"owner": "arushi"}


# ---------- small helpers ----------
def _find_one(patterns: list[str]) -> Path:
    for pat in patterns:
        matches = sorted(glob.glob(str(RAW_DIR / pat)))
        if matches:
            return Path(matches[0])
    raise FileNotFoundError(f"Could not find any file in {RAW_DIR} matching {patterns}")


def _read_csv_tolerant(path: str | Path) -> pd.DataFrame:
    # robust CSV read (mixed types, bad lines)
    df = pd.read_csv(
        path,
        encoding="utf-8",
        on_bad_lines="skip",
        low_memory=False,
        engine="python",
    )
    # normalize column names: lowercase + strip spaces/underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    return df


def _rename_with_synonyms(
    df: pd.DataFrame, mapping: dict[str, list[str]]
) -> pd.DataFrame:
    """
    Given a mapping like {"asin": ["asin","product_id","id"]},
    rename the *first* present synonym to the canonical key.
    """
    rename_dict: dict[str, str] = {}
    for canonical, candidates in mapping.items():
        for c in candidates:
            if c in df.columns:
                rename_dict[c] = canonical
                break
    if rename_dict:
        df = df.rename(columns=rename_dict)
    return df


with DAG(
    dag_id="books_pipeline",
    description="Ingest Amazon books metadata + reviews, transform, merge, Postgres load, analyze, cleanup.",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    max_active_tasks=6,
) as dag:

    # ---------- setup ----------
    @task
    def make_dirs() -> str:
        for d in (RAW_DIR, STAGE_DIR, OUT_DIR):
            d.mkdir(parents=True, exist_ok=True)
        return str(DATA_ROOT)

    # ---------- locate raw files (pattern match; no Kaggle inside Airflow) ----------
    @task
    def fetch_meta(_ok: str) -> str:
        meta = _find_one(
            [
                "amazon_books_metadata_sample_20k.csv",
                "*metadata*20k*.csv",
                "*metadata*.csv",
            ]
        )
        print(f"[fetch_meta] Using metadata file: {meta}")
        return str(meta)

    @task
    def fetch_rev(_ok: str) -> str:
        rev = _find_one(
            [
                "amazon_books_reviews_sample_727k.csv",
                "*reviews*727k*.csv",
                "*reviews*.csv",
            ]
        )
        print(f"[fetch_rev] Using reviews file: {rev}")
        return str(rev)

    # ---------- transform (parallel) ----------
    with TaskGroup("ingest_transform", tooltip="Read and clean CSVs"):

        @task
        def transform_meta(meta_path: str) -> str:
            df = _read_csv_tolerant(meta_path)
            print(f"[transform_meta] Columns (first 30): {list(df.columns)[:30]}")
            print(f"[transform_meta] Head:\n{df.head(3)}")

            # Accept common synonyms and normalize to canonical names
            df = _rename_with_synonyms(
                df,
                mapping={
                    "asin": ["asin", "product_id", "id", "parent_asin"],
                    "title": ["title", "product_title", "book_title", "name"],
                    "authors": ["authors", "author", "contributors"],
                    "categories": ["categories", "category", "genre", "genres"],
                    "price": ["price", "list_price", "amount", "price_usd"],
                    "rating": ["rating", "average_rating", "avg_rating"],
                    "ratingcount": [
                        "ratingcount",
                        "ratings_count",
                        "num_ratings",
                        "review_count",
                    ],
                },
            )

            if "asin" not in df.columns:
                raise ValueError(
                    f"[transform_meta] Required key 'asin' not found. Columns: {list(df.columns)[:30]}"
                )

            keep = [
                c
                for c in [
                    "asin",
                    "title",
                    "authors",
                    "categories",
                    "price",
                    "rating",
                    "ratingcount",
                ]
                if c in df.columns
            ]
            df = df[keep].dropna(subset=["asin"])

            if "authors" in df.columns:
                df["authors"] = (
                    df["authors"].astype(str).str.replace(r"[\[\]']", "", regex=True)
                )
            if "categories" in df.columns:
                df["categories"] = (
                    df["categories"].astype(str).str.replace(r"[\[\]']", "", regex=True)
                )
            if "price" in df.columns:
                df["price"] = pd.to_numeric(df["price"], errors="coerce")

            out = STAGE_DIR / "metadata.parquet"
            df.to_parquet(out, index=False)
            print(f"[transform_meta] Wrote {out} rows={len(df)}")
            return str(out)

        @task
        def transform_rev(rev_path: str) -> str:
            df = _read_csv_tolerant(rev_path)
            print(f"[transform_rev] Columns (first 30): {list(df.columns)[:30]}")
            print(f"[transform_rev] Head:\n{df.head(3)}")

            # Normalize synonyms -> canonical
            df = _rename_with_synonyms(
                df,
                mapping={
                    "asin": ["asin", "product_id", "id", "parent_asin"],
                    "reviewerid": [
                        "reviewerid",
                        "reviewer_id",
                        "profileName",
                        "profile_name",
                        "customer_id",
                    ],
                    "overall": ["overall", "rating", "star_rating", "score"],
                    "unixreviewtime": [
                        "unixreviewtime",
                        "unix_review_time",
                        "unix_time",
                    ],
                    "reviewtime": ["reviewtime", "review_time", "date"],
                },
            )

            if "asin" not in df.columns or "overall" not in df.columns:
                raise ValueError(
                    f"[transform_rev] Required keys 'asin' and 'overall' not found. Columns: {list(df.columns)[:30]}"
                )

            keep = [
                c
                for c in [
                    "asin",
                    "reviewerid",
                    "overall",
                    "unixreviewtime",
                    "reviewtime",
                ]
                if c in df.columns
            ]
            df = df[keep].dropna(subset=["asin"])

            # Build a proper datetime column if available
            if "unixreviewtime" in df.columns:
                df["review_date"] = pd.to_datetime(
                    df["unixreviewtime"], unit="s", errors="coerce", utc=True
                )
            elif "reviewtime" in df.columns:
                df["review_date"] = pd.to_datetime(
                    df["reviewtime"], errors="coerce", utc=True
                )

            out = STAGE_DIR / "reviews.parquet"
            df.to_parquet(out, index=False)
            print(f"[transform_rev] Wrote {out} rows={len(df)}")
            return str(out)

        meta_parquet = transform_meta(fetch_meta(make_dirs()))
        rev_parquet = transform_rev(fetch_rev(make_dirs()))

    # ---------- merge & load ----------
    @task
    def merge_and_load(meta_parquet_path: str, rev_parquet_path: str) -> str:
        meta = pd.read_parquet(meta_parquet_path)
        rev = pd.read_parquet(rev_parquet_path)

        agg = rev.groupby("asin", as_index=False).agg(
            avg_overall=("overall", "mean"), n_reviews=("overall", "size")
        )

        merged = meta.merge(agg, on="asin", how="left").assign(
            avg_overall=lambda d: d["avg_overall"].round(3)
        )

        engine = create_engine(DW_URL, future=True)
        with engine.begin() as conn:
            merged.to_sql("book_summary", con=conn, if_exists="replace", index=False)

        final_path = STAGE_DIR / "book_summary.parquet"
        merged.to_parquet(final_path, index=False)
        print(
            f"[merge_and_load] Loaded to Postgres and wrote {final_path} rows={len(merged)}"
        )
        return str(final_path)

    # ---------- quick analysis ----------
    @task
    def analyze(summary_parquet_path: str) -> str:
        df = pd.read_parquet(summary_parquet_path)
        df["author_primary"] = (
            df.get("authors", "").astype(str).str.split(",").str[0].str.strip()
        )

        top = (
            df.dropna(subset=["avg_overall"])
            .groupby("author_primary", as_index=False)
            .agg(avg_rating=("avg_overall", "mean"), n_titles=("asin", "size"))
            .query("n_titles >= 3")
            .nlargest(10, "avg_rating")
        )

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        fig_path = OUT_DIR / "top_authors.png"
        plt.figure(figsize=(10, 5))
        plt.barh(top["author_primary"][::-1], top["avg_rating"][::-1])
        plt.xlabel("Average Rating (from reviews)")
        plt.title("Top Authors by Average Rating (≥ 3 titles)")
        plt.tight_layout()
        plt.savefig(fig_path, dpi=150)
        plt.close()
        print(f"[analyze] Saved figure {fig_path}")
        return str(fig_path)

    # ---------- cleanup ----------
    @task
    def cleanup(_artifact: str) -> str:
        for p in STAGE_DIR.glob("*.parquet"):
            try:
                p.unlink()
            except Exception as e:
                print(f"Could not delete {p}: {e}")
        return "done"

    # ---------- wiring ----------
    summary = merge_and_load(meta_parquet, rev_parquet)
    plot = analyze(summary)
    cleanup(plot)
