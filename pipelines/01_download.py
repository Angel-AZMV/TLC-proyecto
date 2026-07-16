import os
import sys
import time
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASET_TYPES = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
    "fhvhv":  "fhvhv_tripdata",
}

DEFAULT_YEARS = [2023, 2024, 2025]

MONTHS_PER_YEAR = {
    2023: list(range(1, 13)),
    2024: list(range(1, 13)),
    2025: list(range(1, 13)),
    # 2026: None,
}

BRONZE_PATH = Path("/home/jovyan/data/bronze")
LOG_PATH    = Path("/home/jovyan/data/logs")
MAX_WORKERS = 4
TIMEOUT_SEC = 120
CHUNK_SIZE  = 8192

LOG_PATH.mkdir(parents=True, exist_ok=True)
log_file = LOG_PATH / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def build_download_list(years, dtypes):
    tasks = []
    for year in years:
        months = MONTHS_PER_YEAR.get(year)
        if months is None:
            current_month = datetime.now().month
            months = list(range(1, current_month + 1))
            log.info(f"Año {year}: modo dinámico -> meses 1 a {current_month}")
        for month in months:
            for dtype_key, dtype_prefix in dtypes.items():
                filename = f"{dtype_prefix}_{year}-{month:02d}.parquet"
                url      = f"{BASE_URL}/{filename}"
                dest_dir = BRONZE_PATH / dtype_key / str(year)
                dest     = dest_dir / filename
                tasks.append({
                    "url": url, "dest": dest, "dest_dir": dest_dir,
                    "filename": filename, "year": year, "month": month, "dtype": dtype_key,
                })
    return tasks


def download_file(task):
    url      = task["url"]
    dest     = task["dest"]
    filename = task["filename"]

    if dest.exists() and dest.stat().st_size > 0:
        log.info(f"[SKIP] Ya existe: {filename}")
        return {"status": "skip", "file": filename}

    task["dest_dir"].mkdir(parents=True, exist_ok=True)

    try:
        log.info(f"[DOWN] Descargando: {filename}")
        t0 = time.time()
        response = requests.get(url, stream=True, timeout=TIMEOUT_SEC)
        if response.status_code == 404:
            log.warning(f"[404]  No publicado aun: {filename}")
            return {"status": "not_found", "file": filename}
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
        size_mb = dest.stat().st_size / (1024 * 1024)
        elapsed = time.time() - t0
        log.info(f"[OK]   {filename} -> {size_mb:.1f} MB en {elapsed:.1f}s")
        return {"status": "ok", "file": filename, "size_mb": size_mb}
    except requests.exceptions.Timeout:
        log.error(f"[TIMEOUT] {filename}")
        if dest.exists(): dest.unlink()
        return {"status": "timeout", "file": filename}
    except Exception as e:
        log.error(f"[ERROR] {filename}: {e}")
        if dest.exists(): dest.unlink()
        return {"status": "error", "file": filename, "error": str(e)}


def print_summary(results):
    counts = {"ok": 0, "skip": 0, "not_found": 0, "timeout": 0, "error": 0}
    total_mb = 0.0
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
        total_mb += r.get("size_mb", 0)
    log.info("=" * 50)
    log.info(f"  OK:          {counts['ok']}")
    log.info(f"  Ya existian: {counts['skip']}")
    log.info(f"  No publicados: {counts['not_found']}")
    log.info(f"  Timeouts:    {counts['timeout']}")
    log.info(f"  Errores:     {counts['error']}")
    log.info(f"  Total MB:    {total_mb:.1f}")
    log.info("=" * 50)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",    type=int, default=None)
    parser.add_argument("--dtype",   type=str, default=None, choices=DATASET_TYPES.keys())
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    years = DEFAULT_YEARS.copy()
    if args.year and args.year not in years:
        years.append(args.year)

    dtypes = {args.dtype: DATASET_TYPES[args.dtype]} if args.dtype else DATASET_TYPES.copy()

    log.info(f"Anos a descargar: {years}")
    log.info(f"Tipos: {list(dtypes.keys())}")
    log.info(f"Destino: {BRONZE_PATH}")

    tasks = build_download_list(years, dtypes)
    log.info(f"Total archivos a procesar: {len(tasks)}")

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(download_file, t): t for t in tasks}
        for future in as_completed(futures):
            results.append(future.result())

    print_summary(results)
    errors = sum(1 for r in results if r["status"] in ("error", "timeout"))
    sys.exit(1 if errors > 0 else 0)


if __name__ == "__main__":
    main()
