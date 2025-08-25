import os, glob, psycopg2, csv
from pathlib import Path
import sys
from utility.utility import get_logger, load_config, project_root_from

def get_db_params(cfg):
    env = os.environ
    host = env.get("DB_HOST", cfg["db"]["host"])
    port = int(env.get("DB_PORT", cfg["db"]["port"]))
    db   = env.get("DB_NAME", cfg["db"]["database"])
    user = env.get("DB_USER", cfg["db"]["user"])
    pwd  = env.get("DB_PASS", cfg["db"]["password"])
    return dict(host=host, port=port, database=db, user=user, password=pwd)

def find_single_csv(dirpath):
    paths = sorted(glob.glob(os.path.join(dirpath, "*.csv")))
    if not paths:
        raise FileNotFoundError(f"No CSV found in {dirpath}")
    # If Spark wrote multiple part files, choose the single "part-*.csv" or combine
    # Here we pick the first. For safety we could merge, but we coalesced(1) in transform.
    return paths[0]

def copy_csv(cur, table, csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        # Skip empty lines
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)

def main(config_path: str):
    cfg = load_config(config_path)
    root = project_root_from(__file__)
    log_path = os.path.join(root, "load.log")
    logger = get_logger("load", log_path)

    out_dir = cfg["paths"]["processed_dir"]
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(root, out_dir)

    genre_dir = os.path.join(out_dir, "genre_stats_csv")
    year_dir  = os.path.join(out_dir, "year_stats_csv")
    top_dir   = os.path.join(out_dir, "top_movies_csv")

    genre_csv = find_single_csv(genre_dir)
    year_csv  = find_single_csv(year_dir)
    top_csv   = find_single_csv(top_dir)

    params = get_db_params(cfg)
    logger.info(f"Connecting to postgres at {params['host']}:{params['port']}/{params['database']} ...")
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    # Create tables if not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS genre_stats (
            genre TEXT,
            n_ratings BIGINT,
            avg_rating DOUBLE PRECISION
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS year_stats (
            year INTEGER,
            n_ratings BIGINT,
            avg_rating DOUBLE PRECISION
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS top_movies (
            movie_name TEXT,
            n_ratings BIGINT,
            avg_rating DOUBLE PRECISION
        );
    """)

    # Truncate then load
    for tbl in ("genre_stats","year_stats","top_movies"):
        cur.execute(f"TRUNCATE TABLE {tbl};")

    logger.info("Loading genre_stats ...")
    copy_csv(cur, "genre_stats", genre_csv)
    logger.info("Loading year_stats ...")
    copy_csv(cur, "year_stats", year_csv)
    logger.info("Loading top_movies ...")
    copy_csv(cur, "top_movies", top_csv)

    cur.close()
    conn.close()
    logger.info("Load complete.")

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "./config/config.yaml"
    main(config_path)
