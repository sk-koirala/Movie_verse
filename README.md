# Movies ETL Project (PySpark + PostgreSQL) — End‑to‑End

This repository contains a **complete ETL pipeline** for a (very large) movies ratings dataset packaged as `movies.zip`. It also ships with a **small sample** so you can try everything quickly. The pipeline is split exactly like your screenshot:

```
ETL/
├─ .vscode/
│  └─ launch.json
├─ extract/
│  └─ execute.py
├─ transform/
│  └─ execute.py
├─ load/
│  └─ execute.py
├─ utility/
│  └─ utility.py
├─ config/
│  └─ config.yaml
├─ data/
│  ├─ raw/        # extracted files from movies.zip go here
│  ├─ processed/  # outputs written by transform step
│  └─ sample/     # tiny CSV for quick testing
├─ app/
│  └─ streamlit_app.py
├─ scripts/
│  ├─ docker-compose.yml
│  └─ init_db.sql
├─ execute_all.py
├─ extract.log
├─ transform.log
└─ load.log
```

## 🧩 What does each step do?

- **extract/execute.py**
  - Unzips `movies.zip` into `data/raw/`
  - Optionally creates a small sample CSV for testing (default 20k rows).

- **transform/execute.py** (PySpark)
  - Reads the CSV (sample or full).
  - Cleans columns, extracts `year` from title `(YYYY)`, splits `genre` into multiple rows.
  - Writes:
    - `movies_clean.parquet` (clean dataset for reuse)
    - Aggregations as CSV (for Postgres):
      - `genre_stats_csv/`
      - `year_stats_csv/`
      - `top_movies_csv/`

- **load/execute.py** (PostgreSQL)
  - Creates tables if missing.
  - Bulk loads the CSVs using `COPY`.
  - Tables created: `genre_stats`, `year_stats`, `top_movies`.

- **app/streamlit_app.py** (Visualization)
  - A simple web app to browse the results from Postgres:
    - Bar chart of ratings by **genre**
    - Line chart of ratings by **year**
    - Table of **Top Movies** by avg rating (with minimum review slider)

---

## 🚀 Quick Start (Non‑technical)

> These steps are designed for Windows, macOS, or Linux. No prior experience required.

### 1) Install Python

- Download **Python 3.10+** from https://www.python.org/downloads/  
- During installation, check **"Add Python to PATH"**.

### 2) Install Java (required by PySpark)

- Install **Temurin OpenJDK 11**:
  - Windows/macOS: https://adoptium.net/temurin/releases/
  - Linux (Ubuntu): `sudo apt-get install -y openjdk-11-jdk`
- After installing, restart your terminal.

### 3) Install Docker (to run PostgreSQL easily)

- Install **Docker Desktop**: https://www.docker.com/get-started/
- Start Docker Desktop.

> If you cannot use Docker, you can install PostgreSQL manually and update the DB settings in `config/config.yaml`.

### 4) Download this project

- If you got this as a `movies_etl_project.zip`, **unzip** it to a convenient folder (e.g., Desktop).  
- Put your `movies.zip` (the big dataset file you have) **in the project root**, next to `execute_all.py`.

### 5) Open a terminal in the project folder and run:

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 6) Start PostgreSQL with Docker

```bash
cd scripts
docker compose up -d
cd ..
```

This starts Postgres at `localhost:5432` with:
- database: `movies`
- user: `etl_user`
- password: `etl_pass`

### 7) Run the full ETL

> Make sure `movies.zip` is in the project root.  
> By default we use a **small sample** to keep things fast. You can switch to full dataset later.

```bash
python execute_all.py
```

You will see `extract.log`, `transform.log`, `load.log` being updated.  
On first run, it may take a few minutes depending on your computer.

### 8) Launch the Dashboard

```bash
streamlit run app/streamlit_app.py
```
The browser will open automatically with the dashboard.

---

## 📦 Switching to the Full Dataset

- Open `config/config.yaml` and set:
  - `transform.use_sample_only: false`
- Run again:
  ```bash
  python execute_all.py
  ```

> The full CSV is **huge (≈1.6 GB)** and will take longer. Make sure you have enough disk space and patience.

---

## ⚙️ Configuration

Edit `config/config.yaml`:

- `paths.zip_path`: Where `movies.zip` is. If you place it in the project root, keep it as `"./movies.zip"`.
- `extract.make_sample`: `true/false` to create the small sample automatically.
- `transform.use_sample_only`: `true` (fast) or `false` (full data).
- `db`: Database connection details. Can be overridden by environment variables `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`.

---

## 🧪 VS Code Debug Buttons

Open this folder in **VS Code**. In the **Run and Debug** panel you will see:

- **ETL: Extraction**
- **ETL: Transform**
- **ETL: Load**

These run the three steps individually with the config file.

---

## 🧰 Requirements

Install them with `pip install -r requirements.txt`:

- `pyspark`
- `psycopg2-binary`
- `pandas`
- `pyyaml`
- `streamlit`

---

## ❓FAQ

**I don’t have Docker, can I still use this?**  
Yes. Install PostgreSQL manually and make sure it’s running on port 5432. Update `config/config.yaml` with your credentials.

**Where are the logs?**  
In the project root: `extract.log`, `transform.log`, `load.log`

**Where do outputs go?**  
Spark writes outputs under `data/processed/`:
- `movies_clean.parquet`
- `genre_stats_csv/`
- `year_stats_csv/`
- `top_movies_csv/`

**It says Java not found.**  
Install OpenJDK 11 and restart your terminal.

**I want to change the number of sample rows.**  
Edit `extract.sample_rows` in `config/config.yaml`.

---

## 📜 License

For educational use. You can customize and publish your improvements on your GitHub.
