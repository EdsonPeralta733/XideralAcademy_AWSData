import json
import boto3
import requests
import time

s3 = boto3.client('s3')
BUCKET = "xideralaws-curso-edson"
MIN_FILE_SIZE = 1_000_000  # 1MB mínimo
STATE_FILE = "nyc-taxi/config/last_run.json" # Archivo para recordar el progreso

def check_existing(key):
    # """Verifica si el archivo ya existe en S3 con tamaño válido"""
    try:
        resp = s3.head_object(Bucket=BUCKET, Key=key)
        size = resp['ContentLength']
        if size >= MIN_FILE_SIZE:
            print(f"Ya existe y es válido ({size:,} bytes): {key}")
            return True
        else:
            print(f"Existe pero es muy pequeño ({size:,} bytes), re-descargando: {key}")
            return False
    except:
        return False

def upload_to_s3(url, dataset, year, month, force=False, retries=3):
    # """Descarga y sube archivo Parquet a S3 con reintentos"""
    key = f"nyc-taxi/raw-data/{dataset}/{year}/{month:02d}.parquet"

    if not force and check_existing(key):
        return "skipped"

    if force:
        try:
            s3.delete_object(Bucket=BUCKET, Key=key)
            print(f"Borrado para re-descarga: {key}")
        except:
            pass

    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(1, retries + 1):
        try:
            print(f"[{attempt}/{retries}] Descargando {url}")
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                r.raise_for_status()
                s3.upload_fileobj(r.raw, BUCKET, key)

            resp = s3.head_object(Bucket=BUCKET, Key=key)
            final_size = resp['ContentLength']

            if final_size < MIN_FILE_SIZE:
                print(f"Es muy pequeño ({final_size:,} bytes), reintentando...")
                time.sleep(2)
                continue

            print(f"OK ({final_size:,} bytes = {final_size/(1024**2):.2f} MB): {key}")
            return "ok"

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"No existe en la fuente (404): {url}")
                return "not_found"
            print(f"HTTP error intento {attempt}: {e}")
            time.sleep(2)

        except Exception as e:
            print(f"Error intento {attempt}: {e}")
            time.sleep(2)

    print(f"Falló después de {retries} intentos: {key}")
    return "failed"

def lambda_handler(event, context):
    # """
    #    Lambda Handler para descargar datos NYC Taxi HVFHV
       
    #    Parámetros del evento:
    #    - start_year: Año inicial (default 2021)
    #    - end_year: Año final (default 2025)
    #    - datasets: Lista de datasets (default ["fhvhv"])
    #    - start: Índice de inicio para procesamiento por lotes (default 0)
    #    - batch_size: Cantidad de archivos por ejecución (default 3)
    #    - force: Re-descargar archivos existentes (default False)
    # """
    # 1. INTENTAR LEER EL PROGRESO DESDE S3
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=STATE_FILE)
        state = json.loads(obj['Body'].read().decode('utf-8'))
        start = state.get("siguiente_start", 0)
        print(f"--- Continuando desde el índice: {start} ---")
    except:
        start = event.get("start", 0)
        print(f"--- No hay estado previo, empezando desde: {start} ---")

    start_year = event.get("start_year", 2021)
    end_year   = event.get("end_year", 2025)
    datasets   = event.get("datasets", ["fhvhv"])
    batch_size = event.get("batch_size", 5)
    force      = event.get("force", False)

    # Generar lista de todos los trabajos (dataset/año/mes)
    all_jobs = []
    for dataset in datasets:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{year}-{month:02d}.parquet"
                all_jobs.append((url, dataset, year, month))

    # Procesar solo el lote actual
    total_jobs = len(all_jobs)

    # Si ya terminamos todos, reiniciar a 0 o detener
    if start >= total_jobs:
        print("Todos los archivos han sido procesados.")
        return {"status": "finished"}

    batch = all_jobs[start:start + batch_size]
    print(f"Total jobs: {total_jobs} | Procesando: {start} al {start + len(batch) - 1}")

    # Procesar cada archivo del lote
    results = {"ok": 0, "skipped": 0, "not_found": 0, "failed": 0, "too_small": 0}

    for url, dataset, year, month in batch:
        result = upload_to_s3(url, dataset, year, month, force=force)
        results[result] = results.get(result, 0) + 1

    print(f"Resultados: {results}")

    # Determinar si hay más trabajos pendientes
    siguiente_start = start + batch_size
    hay_mas = siguiente_start < total_jobs

    # 2. GUARDAR EL PROGRESO EN S3 PARA LA PRÓXIMA EJECUCIÓN
    siguiente_start = start + len(batch)
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=STATE_FILE,
            Body=json.dumps({"siguiente_start": siguiente_start})
        )
        print(f"--- Progreso guardado. Siguiente ejecución empezará en: {siguiente_start} ---")
    except Exception as e:
        print(f"Error guardando estado: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "procesados": f"{start} al {siguiente_start - 1}",
            "total_jobs": total_jobs,
            "resultados": results,
            "siguiente_start": siguiente_start,
            "completado": siguiente_start >= total_jobs
        })
    }