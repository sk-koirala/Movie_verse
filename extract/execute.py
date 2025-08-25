import os, zipfile, shutil
from pathlib import Path
import sys
from utility.utility import get_logger, load_config, project_root_from, ensure_dir

def extract_zip(zip_path: str, dest_dir: str, logger):
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Zip not found at {zip_path}")
    ensure_dir(dest_dir)
    logger.info(f"Extracting {zip_path} -> {dest_dir}")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(dest_dir)
    logger.info(f"Extraction complete. Files: {len(os.listdir(dest_dir))} in {dest_dir}")

def make_sample_from_zip(zip_path: str, inner_csv: str, out_csv: str, n_rows: int, logger):
    ensure_dir(Path(out_csv).parent)
    logger.info(f"Creating sample ({n_rows} rows) from {inner_csv} inside {zip_path} -> {out_csv}")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open(inner_csv) as f_in, open(out_csv, "wb") as f_out:
            header = f_in.readline()
            if b"," not in header:
                # If there's a weird header, still pass it through
                pass
            f_out.write(header)
            for i, line in enumerate(f_in, start=1):
                f_out.write(line)
                if i >= n_rows:
                    break
    logger.info(f"Sample written: {out_csv}")

def main(config_path: str):
    cfg = load_config(config_path)
    root = project_root_from(__file__)
    log_path = os.path.join(root, "extract.log")
    logger = get_logger("extract", log_path)

    zip_path = cfg["paths"].get("zip_path", "./movies.zip")
    # If zip path is relative, resolve from project root
    if not os.path.isabs(zip_path):
        zip_path = os.path.join(root, zip_path)

    raw_dir = cfg["paths"]["raw_dir"]
    if not os.path.isabs(raw_dir):
        raw_dir = os.path.join(root, raw_dir)

    # Do extraction
    extract_zip(zip_path, raw_dir, logger)

    # Optional: small sample CSV to speed up dev/testing
    if cfg.get("extract", {}).get("make_sample", True):
        inner_csv = cfg["transform"].get("csv_filename_inside_zip", "movies_dataset.csv")
        sample_rows = int(cfg.get("extract", {}).get("sample_rows", 20000))
        out_csv = cfg["paths"].get("sample_csv", "./data/sample/movies_dataset_sample.csv")
        if not os.path.isabs(out_csv):
            out_csv = os.path.join(root, out_csv)
        make_sample_from_zip(zip_path, inner_csv, out_csv, sample_rows, logger)

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "./config/config.yaml"
    main(config_path)
