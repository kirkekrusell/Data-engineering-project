import requests
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
from pathlib import Path
import pandas as pd
#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from airflow.utils.dates import days_ago

# === CONFIG ===
DATA_DIR = "/andmetehnika/data"
EBR_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip"
MTR_URL = "https://mtr.ttja.ee/taotluse_tulemus/csv/action"
lingid = ["https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
         "https://mtr.ttja.ee/taotluse_tulemus/csv/action"]
nimi = ["ebr", "mtr"]
local_filename = []

def download_and_validate():
    date_str = datetime.now().strftime("%Y-%m-%d")
    for n in nimi:
        versioon = f"{n}_{date_str}.csv"
        local_filename.append(versioon)

#download_and_validate()

#with requests.get(EBR_URL, stream=True, timeout=1000) as r:
    #print(r.raise_for_status())
def download_and_extract_zip(url, dest_dir='.', filename=None, headers=None, retries=3, timeout=30):
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    if filename is None:
        filename = url.split('/')[-1] or 'download.zip'
    zip_path = dest_dir / filename

    # session with retry
    s = requests.Session()
    retries_policy = Retry(total=retries, backoff_factor=1,
                           status_forcelist=(429, 500, 502, 503, 504),
                           allowed_methods=frozenset(['GET']))
    s.mount("https://", HTTPAdapter(max_retries=retries_policy))
    s.mount("http://", HTTPAdapter(max_retries=retries_policy))

    # sensible headers to avoid 403
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Referer": "https://avaandmed.ariregister.rik.ee/"
    }
    if headers:
        default_headers.update(headers)

    with s.get(url, headers=default_headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    # extract CSV(s)
    extracted_files = []
    with zipfile.ZipFile(zip_path, 'r') as z:
        for info in z.infolist():
            if info.filename.lower().endswith('.csv'):
                target = dest_dir / Path(info.filename).name
                with z.open(info) as src, open(target, 'wb') as dst:
                    dst.write(src.read())
                extracted_files.append(str(target))

    return extracted_files  # list of extracted CSV file paths

# usage
csv_files = download_and_extract_zip(EBR_URL, dest_dir='data')
print('extracted:', csv_files)


"""
with DAG(
    dag_id="data_downloader_and_quality_checker",
    start_date=days_ago,
    schedule_interval="0 0 * * 0",  # once a week at midnight on Sunday morning
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id="download_file",
        python_callable=download_and_validate,
        provide_context=True
    )
"""
