#!/usr/bin/env python3
"""
NYC Taxi Data Cleaning con PySpark en EC2
Lee desde S3 raw-data → Limpia → Guarda en processed-data
"""
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import boto3

# Configuración
BUCKET = "xideralaws-curso-edson"
DATASET = "fhvhv"

def create_spark_session():
    """Crea sesión Spark optimizada para EC2 con soporte S3"""
    return SparkSession.builder \
        .appName("NYC-Taxi-Clean") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def clean_data(df):
    """Limpieza y validación de datos NYC Taxi"""
    print(f"📊 Registros originales: {df.count():,}")

    # 1. Eliminar nulos en columnas críticas
    df_clean = df.dropna(subset=[
        'pickup_datetime', 'dropoff_datetime',
        'PULocationID', 'DOLocationID'
    ])
    print(f"✅ Sin nulos críticos: {df_clean.count():,}")

    # 2. Validar fechas lógicas
    df_clean = df_clean.filter(
        F.col('pickup_datetime') < F.col('dropoff_datetime')
    )
    print(f"✅ Fechas válidas: {df_clean.count():,}")

    # 3. Filtrar valores válidos
    df_clean = df_clean.filter(
        (F.col('trip_miles') > 0) &
        (F.col('trip_miles') < 500) &
        (F.col('base_passenger_fare') >= 0) &
        (F.col('base_passenger_fare') < 1000)
    )
    print(f"✅ Valores válidos: {df_clean.count():,}")

    # 4. Rellenar nulos en columnas numéricas
    numeric_cols = ['tolls', 'bcf', 'sales_tax', 'congestion_surcharge',
                    'airport_fee', 'tips', 'driver_pay']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean = df_clean.fillna({col: 0.0})

    # 5. Normalizar flags a booleanos
    flag_cols = ['shared_request_flag', 'shared_match_flag',
                 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag']
    for col in flag_cols:
        if col in df_clean.columns:
            df_clean = df_clean.withColumn(
                col, F.when(F.col(col) == 'Y', True).otherwise(False)
            )

    # 6. Columnas derivadas útiles
    df_clean = df_clean.withColumn(
        'trip_duration_minutes',
        (F.unix_timestamp('dropoff_datetime') -
        F.unix_timestamp('pickup_datetime')) / 60
    )

    df_clean = df_clean.withColumn(
        'total_amount',
        F.col('base_passenger_fare') +
        F.coalesce(F.col('tolls'), F.lit(0)) +
        F.coalesce(F.col('sales_tax'), F.lit(0)) +
        F.coalesce(F.col('congestion_surcharge'), F.lit(0)) +
        F.coalesce(F.col('airport_fee'), F.lit(0)) +
        F.coalesce(F.col('tips'), F.lit(0))
    )

    print(f"✅ Registros finales limpios: {df_clean.count():,}")
    return df_clean

def generate_quality_report(df_original, df_clean):
    """Genera reporte JSON de calidad"""
    original_count = df_original.count()
    clean_count = df_clean.count()

    stats = df_clean.select(
        F.avg('trip_miles').alias('avg_miles'),
        F.avg('trip_duration_minutes').alias('avg_duration'),
        F.avg('base_passenger_fare').alias('avg_fare'),
        F.avg('tips').alias('avg_tips'),
        F.sum('base_passenger_fare').alias('total_revenue'),
        F.count('*').alias('total_trips')
    ).collect()[0]

    return {
        "timestamp": datetime.now().isoformat(),
        "data_quality": {
            "original_records": original_count,
            "clean_records": clean_count,
            "removed_records": original_count - clean_count,
            "quality_score_percent": round((clean_count / original_count) * 100, 2)
        },
        "statistics": {
            "total_trips": int(stats['total_trips']),
            "avg_trip_miles": round(float(stats['avg_miles'] or 0), 2),
            "avg_trip_duration_min": round(float(stats['avg_duration'] or 0), 2),
            "avg_fare_usd": round(float(stats['avg_fare'] or 0), 2),
            "avg_tips_usd": round(float(stats['avg_tips'] or 0), 2),
            "total_revenue_usd": round(float(stats['total_revenue'] or 0), 2)
        }
    }

def process_file(spark, s3_client, year, month):
    """Procesa un archivo Parquet individual"""
    try:
        print(f"\n{'='*70}")
        print(f"🔍 Procesando: {year}/{month}")
        print(f"{'='*70}")

        # Rutas S3
        input_path = f"s3a://{BUCKET}/nyc-taxi/raw-data/{DATASET}/{year}/{month}.parquet"
        output_path = f"s3a://{BUCKET}/nyc-taxi/processed-data/cleaned/{DATASET}/{year}/"
        report_key = f"nyc-taxi/processed-data/quality-reports/{DATASET}/{year}-{month}.json"

        # 1. Leer datos crudos
        print(f"📥 Leyendo: {input_path}")
        df_original = spark.read.parquet(input_path)

        # 2. Limpiar datos
        df_clean = clean_data(df_original)

        # 3. Generar reporte de calidad
        report = generate_quality_report(df_original, df_clean)

        # 4. Guardar datos limpios (1 archivo por mes)
        print(f"📤 Guardando datos limpios...")
        df_clean.coalesce(1).write.mode('overwrite').parquet(
            output_path + f"{month}.parquet"
        )
        print(f"✅ Guardado: {output_path}{month}.parquet")

        # 5. Guardar reporte de calidad en S3
        s3_client.put_object(
            Bucket=BUCKET,
            Key=report_key,
            Body=json.dumps(report, indent=2),
            ContentType='application/json'
        )
        print(f"📊 Reporte guardado: {report_key}")

        # Mostrar resumen
        print(f"\n📈 RESUMEN:")
        print(f"   Quality Score: {report['data_quality']['quality_score_percent']}%")
        print(f"   Records: {report['data_quality']['clean_records']:,}")
        print(f"   Avg Trip: {report['statistics']['avg_trip_miles']} miles")
        print(f"   Avg Fare: ${report['statistics']['avg_fare_usd']}")

        return {
            "status": "success",
            "year": year,
            "month": month,
            "quality_score": report['data_quality']['quality_score_percent']
        }

    except Exception as e:
        print(f"❌ ERROR procesando {year}/{month}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "failed",
            "year": year,
            "month": month,
            "error": str(e)
        }

def main():
    """Función principal"""
    print("🚀 NYC Taxi Data Cleaning - PySpark on EC2")
    print("=" * 70)

    # Crear Spark session
    print("🔧 Iniciando Spark Session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Menos logs verbosos

    s3_client = boto3.client('s3')

    # Determinar qué procesar
    if len(sys.argv) >= 3:
        # Modo: python3 clean_data.py 2024 01
        year = sys.argv[1]
        month = sys.argv[2]
        print(f"📋 Modo: Archivo único ({year}/{month})")
        result = process_file(spark, s3_client, year, month)

    elif len(sys.argv) == 2 and sys.argv[1] == "all":
        # Modo: python3 clean_data.py all
        print(f"📋 Modo: Procesar TODOS los archivos (2021-2025)")
        results = {"success": 0, "failed": 0}
        failed_files = []

        for year in range(2021, 2026):
            for month in range(1, 13):
                result = process_file(spark, s3_client, str(year), f"{month:02d}")
                if result["status"] == "success":
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    failed_files.append(f"{year}/{month:02d}")

        print(f"\n{'='*70}")
        print(f"🎉 PROCESO COMPLETADO")
        print(f"{'='*70}")
        print(f"✅ Exitosos: {results['success']}")
        print(f"❌ Fallidos: {results['failed']}")
        if failed_files:
            print(f"📋 Archivos fallidos: {', '.join(failed_files)}")

    else:
        print("❌ Uso incorrecto")
        print("\nOpciones:")
        print("  1. Archivo específico: python3 clean_data.py 2024 01")
        print("  2. Todos los archivos: python3 clean_data.py all")
        sys.exit(1)

    spark.stop()
    print("\n✨ Spark Session cerrada correctamente")

if __name__ == "__main__":
    main()