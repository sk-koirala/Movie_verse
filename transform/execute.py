import os, re
from pathlib import Path
import sys
from utility.utility import get_logger, load_config, project_root_from, ensure_dir, get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, ArrayType

def read_movies_df(spark, cfg, root):
    use_sample = bool(cfg.get("transform", {}).get("use_sample_only", True))
    if use_sample:
        path = cfg["paths"]["sample_csv"]
    else:
        # Look for extracted CSV in raw_dir
        csv_name = cfg["transform"].get("csv_filename_inside_zip", "movies_dataset.csv")
        raw_dir = cfg["paths"]["raw_dir"]
        path = os.path.join(raw_dir, csv_name)

    if not os.path.isabs(path):
        path = os.path.join(root, path)

    opts = cfg["transform"].get("csv_options", {"header": True, "inferSchema": True})
    df = (spark.read.options(**{k:str(v).lower() if isinstance(v, bool) else v for k,v in opts.items()})
                  .csv(path))
    return df

def clean_and_enrich(df):
    # Standardize column names
    cols = [c.strip() for c in df.columns]
    # Some zips include unnamed index column: '', 'Unnamed: 0', '_c0'
    rename_map = {}
    drop_cols = []
    for c in cols:
        if c in ("", "Unnamed: 0", "_c0", "index"):
            drop_cols.append(c)
        elif c.lower() == "user_id":
            rename_map[c] = "user_id"
        elif c.lower() in ("movie_name","title","movie","movie_title"):
            rename_map[c] = "movie_name"
        elif c.lower() in ("rating","rate","stars"):
            rename_map[c] = "rating"
        elif c.lower() == "genre":
            rename_map[c] = "genre"
    for old,new in rename_map.items():
        df = df.withColumnRenamed(old,new)
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)

    # Ensure needed columns exist
    required = ["user_id","movie_name","rating","genre"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Required column missing: {col}. Columns present: {df.columns}")

    df = df.withColumn("user_id", F.col("user_id").cast(IntegerType())) \
           .withColumn("rating", F.col("rating").cast(DoubleType())) \
           .withColumn("movie_name", F.col("movie_name").cast(StringType())) \
           .withColumn("genre", F.col("genre").cast(StringType()))

    # Extract year from movie_name: look for (YYYY)
    year_regex = F.regexp_extract(F.col("movie_name"), r"\((\d{4})\)", 1)
    df = df.withColumn("year", F.when(year_regex != "", year_regex.cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType())))

    # Genres split + explode
    df = df.withColumn("genres_array", F.split(F.col("genre"), r"\|"))
    exploded = df.withColumn("genre_exploded", F.explode_outer("genres_array"))

    return df, exploded

def write_outputs(df, exploded, out_dir):
    ensure_dir(out_dir)

    # Save clean dataset (parquet) for reuse
    df.write.mode("overwrite").parquet(os.path.join(out_dir, "movies_clean.parquet"))

    # Aggregations
    genre_stats = (exploded.groupBy("genre_exploded")
                           .agg(F.count("*").alias("n_ratings"),
                                F.avg("rating").alias("avg_rating"))
                           .withColumnRenamed("genre_exploded","genre")
                           .orderBy(F.desc("n_ratings")))

    year_stats = (df.groupBy("year")
                    .agg(F.count("*").alias("n_ratings"),
                         F.avg("rating").alias("avg_rating"))
                    .orderBy("year"))

    top_movies = (df.groupBy("movie_name")
                    .agg(F.count("*").alias("n_ratings"),
                         F.avg("rating").alias("avg_rating"))
                    .filter(F.col("n_ratings") >= 10)
                    .orderBy(F.desc("avg_rating"), F.desc("n_ratings"))
                 )

    # Write CSVs for loading to Postgres
    (genre_stats.coalesce(1)
        .write.mode("overwrite").option("header","true")
        .csv(os.path.join(out_dir, "genre_stats_csv")))

    (year_stats.coalesce(1)
        .write.mode("overwrite").option("header","true")
        .csv(os.path.join(out_dir, "year_stats_csv")))

    (top_movies.coalesce(1)
        .write.mode("overwrite").option("header","true")
        .csv(os.path.join(out_dir, "top_movies_csv")))

def main(config_path: str):
    from utility.utility import get_logger, load_config, project_root_from

    cfg = load_config(config_path)
    root = project_root_from(__file__)
    log_path = os.path.join(root, "transform.log")
    logger = get_logger("transform", log_path)

    spark = get_spark("MoviesETL-Transform")

    logger.info("Reading movies dataframe...")
    df = read_movies_df(spark, cfg, root)
    logger.info(f"Initial columns: {df.columns} | Count (sampled): {df.count()}")

    logger.info("Cleaning + enriching...")
    df_clean, df_exploded = clean_and_enrich(df)

    out_dir = cfg["paths"]["processed_dir"]
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(root, out_dir)

    logger.info(f"Writing outputs to {out_dir} ...")
    write_outputs(df_clean, df_exploded, out_dir)
    logger.info("Transform step complete.")

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "./config/config.yaml"
    main(config_path)
