from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import zipfile
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === CONFIG ===
EBR_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip"
DATA_DIR = "/opt/airflow/data"

def download_and_extract_zip():
    dest_dir = Path(DATA_DIR)
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = EBR_URL.split('/')[-1] or 'ebr.zip'
    zip_path = dest_dir / filename

    # session with retry
    s = requests.Session()
    retries_policy = Retry(total=3, backoff_factor=1,
                           status_forcelist=(429, 500, 502, 503, 504),
                           allowed_methods=frozenset(['GET']))
    s.mount("https://", HTTPAdapter(max_retries=retries_policy))
    s.mount("http://", HTTPAdapter(max_retries=retries_policy))

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://avaandmed.ariregister.rik.ee/"
    }

    with s.get(EBR_URL, headers=headers, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    # extract CSV(s)
    with zipfile.ZipFile(zip_path, 'r') as z:
        for info in z.infolist():
            if info.filename.lower().endswith('.csv'):
                target = dest_dir / Path(info.filename).name
                with z.open(info) as src, open(target, 'wb') as dst:
                    dst.write(src.read())
                print(f"Ekstraktitud: {target}")

# === DAG ===
with DAG(
    dag_id="data_downloader_and_quality_checker",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 * * 0",  # kord nädalas pühapäeval
    catchup=False,
    tags=["avaandmed", "csv"]
) as dag:

    download_task = PythonOperator(
        task_id="download_ebr_csv",
        python_callable=download_and_extract_zip
    )
