import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
from pathlib import Path
from typing import List, Union, Optional

# Optional: pandas only required when return_type='dataframe'
try:
    import pandas as pd
except Exception:
    pd = None

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Referer": "https://avaandmed.ariregister.rik.ee/"
}

def _make_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    s = requests.Session()
    retries_policy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET'])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries_policy))
    s.mount("http://", HTTPAdapter(max_retries=retries_policy))
    return s

def _is_zip_by_url(url: str) -> bool:
    return url.lower().rstrip('/').endswith(('.zip', '.csv.zip'))

def _choose_csv_from_zip(z: zipfile.ZipFile, preferred_name: Optional[str] = None) -> str:
    # return the name of the CSV file inside the zip
    names = [n for n in z.namelist() if n and not n.endswith('/') and n.lower().endswith('.csv')]
    if not names:
        raise RuntimeError("No CSV file found inside the ZIP archive")
    if preferred_name:
        for n in names:
            if preferred_name in Path(n).name:
                return n
    # prefer shortest (usually top-level) or first
    names.sort(key=lambda s: (s.count('/'), len(s)))
    return names[0]

def fetch_csv_from_url(
    url: str,
    dest_dir: Union[str, Path] = ".",
    filename: Optional[str] = None,
    return_type: str = "path",   # 'path' | 'dataframe' | 'text'
    prefer_memory: bool = False, # for small files: download into memory instead of streaming to disk
    headers: Optional[dict] = None,
    retries: int = 3,
    timeout: int = 30,
    csv_in_zip_name: Optional[str] = None  # optional substring to pick a specific csv inside zip
) -> Union[str, List[str], 'pd.DataFrame', List['pd.DataFrame'], str]:
    """
    Universal CSV retriever:
    - If `url` is a direct CSV, downloads it (streamed) and returns a path or DataFrame (depending on return_type).
    - If `url` is a zip containing CSV(s), extracts the CSV(s) and returns path(s) or DataFrame(s).
    Parameters:
      - url: the http(s) URL to fetch (zip or csv)
      - dest_dir: directory to write files when return_type='path' or streaming
      - filename: optional filename to save zip or csv (if omitted derived from URL)
      - return_type: 'path' (default) returns local path(s); 'dataframe' returns pandas DataFrame(s); 'text' returns CSV text
      - prefer_memory: if True and file small, process in-memory (BytesIO) instead of writing zip to disk
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    headers = {**DEFAULT_HEADERS, **(headers or {})}

    session = _make_session(retries=retries)

    # Quick decision by URL extension - good heuristic (we also check content-type later)
    looks_like_zip = _is_zip_by_url(url)

    # If user wants in-memory and small file, we will download r.content (careful with large files)
    if prefer_memory:
        r = session.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        if 'zip' in ctype or looks_like_zip:
            # open zip from bytes
            bio = io.BytesIO(r.content)
            with zipfile.ZipFile(bio) as z:
                csv_name = _choose_csv_from_zip(z, preferred_name=csv_in_zip_name)
                with z.open(csv_name) as fh:
                    if return_type == 'path':
                        out_name = filename or Path(csv_name).name
                        out_path = dest_dir / out_name
                        with open(out_path, 'wb') as out_f:
                            out_f.write(fh.read())
                        return str(out_path)
                    elif return_type == 'dataframe':
                        if pd is None:
                            raise RuntimeError("pandas is required for return_type='dataframe'")
                        return pd.read_csv(fh)
                    elif return_type == 'text':
                        return fh.read().decode('utf-8', errors='replace')
                    else:
                        raise ValueError("Unsupported return_type")
        else:
            # treat as csv
            if return_type == 'path':
                out_name = filename or Path(url).name or "download.csv"
                out_path = dest_dir / out_name
                with open(out_path, 'wb') as out_f:
                    out_f.write(r.content)
                return str(out_path)
            elif return_type == 'dataframe':
                if pd is None:
                    raise RuntimeError("pandas is required for return_type='dataframe'")
                bio = io.BytesIO(r.content)
                return pd.read_csv(bio)
            elif return_type == 'text':
                return r.text
            else:
                raise ValueError("Unsupported return_type")

    # Otherwise, prefer streaming to disk (safer for large files)
    # We'll stream the response and decide by content-type or filename whether it's a zip
    with session.get(url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        # choose filename for download
        derived_name = filename or Path(url.split('?')[0]).name or ("download.zip" if looks_like_zip else "download.csv")
        temp_path = dest_dir / derived_name

        # If server says zip or url endswith .zip -> stream to disk and extract
        is_zip = 'zip' in ctype or looks_like_zip or derived_name.lower().endswith('.zip')
        if is_zip:
            # stream zip to file
            with open(temp_path, 'wb') as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
            # open the zip and extract the desired csv
            extracted_paths = []
            with zipfile.ZipFile(temp_path, 'r') as z:
                csv_name = _choose_csv_from_zip(z, preferred_name=csv_in_zip_name)
                with z.open(csv_name) as csv_fh:
                    if return_type == 'path':
                        out_name = Path(csv_name).name
                        out_path = dest_dir / out_name
                        with open(out_path, 'wb') as out_f:
                            out_f.write(csv_fh.read())
                        extracted_paths.append(str(out_path))
                    elif return_type == 'dataframe':
                        if pd is None:
                            raise RuntimeError("pandas is required for return_type='dataframe'")
                        # pandas can read from a file-like object opened in binary mode
                        df = pd.read_csv(csv_fh)
                        return df
                    elif return_type == 'text':
                        return csv_fh.read().decode('utf-8', errors='replace')
                    else:
                        raise ValueError("Unsupported return_type")
            return extracted_paths if len(extracted_paths) != 1 else extracted_paths[0]
        else:
            # Direct CSV: stream to disk or stream into pandas (if requested)
            if return_type == 'path':
                out_path = dest_dir / derived_name
                with open(out_path, 'wb') as out_f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            out_f.write(chunk)
                return str(out_path)
            elif return_type == 'dataframe':
                if pd is None:
                    raise RuntimeError("pandas is required for return_type='dataframe'")
                # pandas can read directly from the response raw stream
                # use response.raw (ensure stream=True earlier)
                r.raw.decode_content = True
                return pd.read_csv(r.raw)
            elif return_type == 'text':
                return r.text
            else:
                raise ValueError("Unsupported return_type")
            


def download_and_validate():
    nimi = ["ebr", "mtr"]
    local_filename = []
    date_str = datetime.now().strftime("%Y-%m-%d")
    for n in nimi:
        versioon = f"{n}_{date_str}.csv"
        local_filename.append(versioon)
    return local_filename



def main():
    lingid = ["https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
            "https://mtr.ttja.ee/taotluse_tulemus/csv/action"]
    failinimed = download_and_validate()
    for index, url in enumerate(lingid):
        result = fetch_csv_from_url(
            url,
            dest_dir='data',
            filename=failinimed[index],
            return_type='path',
            prefer_memory=False,
            retries=3,
            timeout=60,
            csv_in_zip_name=None
        )
        print(f"Downloaded file saved to: {result}")


if __name__ == "__main__":
    main()