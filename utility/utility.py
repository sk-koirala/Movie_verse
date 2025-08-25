import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import yaml
from pathlib import Path

def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def get_logger(name: str, log_file: str):
    ensure_dir(Path(log_file).parent)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Avoid adding multiple handlers in REPL/debug
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=2)
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

def load_config(config_path: str):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg

def project_root_from(current_file: str):
    # /.../extract/execute.py -> /.../
    return str(Path(current_file).resolve().parent.parent)

def get_spark(app_name="MoviesETL", master="local[*]"):
    # Lazy import so this file can be used without pyspark until transform step
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master)
               .config("spark.sql.session.timeZone","UTC")
               .config("spark.sql.shuffle.partitions","8"))
    spark = builder.getOrCreate()
    return spark
