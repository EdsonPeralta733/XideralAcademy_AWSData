#!/usr/bin/env python3
"""
NYC Taxi Data Analysis - Por Año Independiente
Analiza cada año por separado para evitar conflictos de schema
"""

import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import boto3

BUCKET = "xideralaws-curso-edson"
DATASET = "fhvhv"

def create_spark_session():
    """Crea sesión Spark para análisis"""
    return SparkSession.builder \
        .appName("NYC-Taxi-Analysis") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def load_year_data(spark, year):
    """Carga todos los datos de un año específico"""
    print(f"📥 Cargando datos del año {year}...")

    year_path = f"s3a://{BUCKET}/nyc-taxi/processed-data/cleaned/{DATASET}/{year}/*/*.parquet"
    df = spark.read.parquet(year_path)

    total_records = df.count()
    print(f"✅ Año {year}: {total_records:,} registros")

    return df

def load_specific_month(spark, year, month):
    """Carga datos de un mes específico"""
    print(f"📥 Cargando datos de {year}/{month}...")

    file_path = f"s3a://{BUCKET}/nyc-taxi/processed-data/cleaned/{DATASET}/{year}/{month}.parquet/*"
    df = spark.read.parquet(file_path)

    total_records = df.count()
    print(f"✅ Registros cargados: {total_records:,}")

    return df

def generate_summary(df, year=None):
    """Genera resumen del DataFrame"""
    print(f"\n📊 Generando resumen{'del año ' + str(year) if year else ''}...")

    # Verificar columnas disponibles
    has_shared = 'shared_request_flag' in df.columns
    has_tips = 'tips' in df.columns
    has_locations = 'PULocationID' in df.columns and 'DOLocationID' in df.columns

    # Agregaciones básicas
    agg_list = [
        F.count("*").alias("total_trips"),
        F.sum("base_passenger_fare").alias("total_revenue"),
        F.avg("base_passenger_fare").alias("avg_fare"),
        F.avg("trip_miles").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration")
    ]

    if has_tips:
        agg_list.extend([
            F.sum("tips").alias("total_tips"),
            F.avg("tips").alias("avg_tips")
        ])

    if has_shared:
        agg_list.append(
            F.sum(F.when(F.col("shared_request_flag") == True, 1).otherwise(0)).alias("shared_trips")
        )

    result = df.agg(*agg_list).collect()[0]

    summary = {
        "total_trips": int(result.total_trips),
        "total_revenue": round(float(result.total_revenue), 2),
        "avg_fare": round(float(result.avg_fare), 2),
        "avg_distance": round(float(result.avg_distance), 2),
        "avg_duration": round(float(result.avg_duration), 2)
    }

    if has_tips:
        summary["total_tips"] = round(float(result.total_tips), 2)
        summary["avg_tips"] = round(float(result.avg_tips), 2)

    if has_shared:
        summary["shared_trips"] = int(result.shared_trips)
        summary["shared_trip_rate"] = round((int(result.shared_trips) / int(result.total_trips)) * 100, 2)

    if year:
        summary["year"] = year

    print(f"✅ Total viajes: {summary['total_trips']:,}")
    print(f"✅ Ingresos: ${summary['total_revenue']:,.2f}")

    return summary

def generate_monthly_metrics(df, year):
    """Genera métricas por mes dentro de un año"""
    print(f"\n📊 Generando métricas mensuales del año {year}...")

    df_monthly = df.withColumn("month", F.month("pickup_datetime"))

    has_tips = 'tips' in df.columns
    has_shared = 'shared_request_flag' in df.columns

    agg_list = [
        F.count("*").alias("total_trips"),
        F.sum("base_passenger_fare").alias("total_revenue"),
        F.avg("base_passenger_fare").alias("avg_fare"),
        F.avg("trip_miles").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration")
    ]

    if has_tips:
        agg_list.extend([
            F.sum("tips").alias("total_tips"),
            F.avg("tips").alias("avg_tips")
        ])

    if has_shared:
        agg_list.append(
            F.sum(F.when(F.col("shared_request_flag") == True, 1).otherwise(0)).alias("shared_trips")
        )

    monthly_stats = df_monthly.groupBy("month").agg(*agg_list).orderBy("month")

    monthly_data = []
    for row in monthly_stats.collect():
        data = {
            "year": year,
            "month": int(row.month),
            "total_trips": int(row.total_trips),
            "total_revenue": round(float(row.total_revenue), 2),
            "avg_fare": round(float(row.avg_fare), 2),
            "avg_distance": round(float(row.avg_distance), 2),
            "avg_duration": round(float(row.avg_duration), 2)
        }

        if has_tips:
            data["total_tips"] = round(float(row.total_tips), 2)
            data["avg_tips"] = round(float(row.avg_tips), 2)

        if has_shared:
            data["shared_trips"] = int(row.shared_trips)
            data["shared_trip_rate"] = round((int(row.shared_trips) / int(row.total_trips)) * 100, 2)

        monthly_data.append(data)

    print(f"✅ {len(monthly_data)} meses procesados")
    return monthly_data

def generate_hourly_patterns(df):
    """Analiza patrones por hora del día"""
    print(f"\n📊 Analizando patrones horarios...")

    df_hourly = df.withColumn("hour", F.hour("pickup_datetime"))

    hourly_stats = df_hourly.groupBy("hour").agg(
        F.count("*").alias("total_trips"),
        F.avg("base_passenger_fare").alias("avg_fare"),
        F.avg("trip_miles").alias("avg_distance")
    ).orderBy("hour")

    hourly_data = []
    for row in hourly_stats.collect():
        hourly_data.append({
            "hour": int(row.hour),
            "total_trips": int(row.total_trips),
            "avg_fare": round(float(row.avg_fare), 2),
            "avg_distance": round(float(row.avg_distance), 2)
        })

    print(f"✅ 24 horas analizadas")
    return hourly_data

def generate_day_of_week_patterns(df):
    """Analiza patrones por día de la semana"""
    print(f"\n📊 Analizando patrones por día...")

    df_dow = df.withColumn("day_of_week", F.dayofweek("pickup_datetime"))

    dow_stats = df_dow.groupBy("day_of_week").agg(
        F.count("*").alias("total_trips"),
        F.avg("base_passenger_fare").alias("avg_fare"),
        F.avg("trip_miles").alias("avg_distance")
    ).orderBy("day_of_week")

    day_names = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]

    dow_data = []
    for row in dow_stats.collect():
        dow_data.append({
            "day_of_week": int(row.day_of_week),
            "day_name": day_names[int(row.day_of_week) - 1],
            "total_trips": int(row.total_trips),
            "avg_fare": round(float(row.avg_fare), 2),
            "avg_distance": round(float(row.avg_distance), 2)
        })

    print(f"✅ 7 días analizados")
    return dow_data

def analyze_year(spark, s3_client, year):
    """Analiza un año completo de forma independiente"""
    print(f"\n{'='*70}")
    print(f"📅 ANALIZANDO AÑO {year}")
    print(f"{'='*70}")

    try:
        # Cargar datos del año
        df = load_year_data(spark, year)

        # Generar análisis
        results = {
            "year": year,
            "timestamp": datetime.now().isoformat(),
            "summary": generate_summary(df, year),
            "monthly_metrics": generate_monthly_metrics(df, year),
            "hourly_patterns": generate_hourly_patterns(df),
            "day_of_week_patterns": generate_day_of_week_patterns(df)
        }

        # Guardar resultados del año
        output_key = f"nyc-taxi/analysis-results/by-year/{year}_analysis.json"
        s3_client.put_object(
            Bucket=BUCKET,
            Key=output_key,
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )

        print(f"\n✅ Análisis del año {year} guardado: {output_key}")
        return results

    except Exception as e:
        print(f"❌ Error analizando año {year}: {str(e)}")
        return None

def main():
    """Función principal"""
    print("🚀 NYC Taxi Data Analysis - Por Año")
    print("=" * 70)

    print("🔧 Iniciando Spark Session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    s3_client = boto3.client('s3')

    try:
        # Determinar modo de ejecución
        if len(sys.argv) >= 3:
            # MODO: Análisis de mes específico
            year = sys.argv[1]
            month = sys.argv[2]
            print(f"📋 Modo: Análisis de mes específico ({year}/{month})\n")

            df = load_specific_month(spark, year, month)

            results = {
                "year": int(year),
                "month": int(month),
                "timestamp": datetime.now().isoformat(),
                "summary": generate_summary(df),
                "hourly_patterns": generate_hourly_patterns(df),
                "day_of_week_patterns": generate_day_of_week_patterns(df)
            }

            output_key = f"nyc-taxi/analysis-results/monthly/{year}-{month}_analysis.json"
            s3_client.put_object(
                Bucket=BUCKET,
                Key=output_key,
                Body=json.dumps(results, indent=2),
                ContentType='application/json'
            )
            print(f"\n✅ Análisis guardado: s3://{BUCKET}/{output_key}")

        elif len(sys.argv) == 2:
            # MODO: Análisis de un año específico
            year = int(sys.argv[1])
            print(f"📋 Modo: Análisis de año completo ({year})\n")

            analyze_year(spark, s3_client, year)

        else:
            # MODO: Análisis de todos los años
            print(f"📋 Modo: Análisis de TODOS los años (2021-2025)\n")

            years = [2021, 2022, 2023, 2024, 2025]
            all_results = []

            for year in years:
                result = analyze_year(spark, s3_client, year)
                if result:
                    all_results.append(result)

            # Crear índice consolidado (solo métricas, no DataFrames)
            index = {
                "timestamp": datetime.now().isoformat(),
                "total_years": len(all_results),
                "years_analyzed": [r["year"] for r in all_results],
                "yearly_summaries": [r["summary"] for r in all_results]
            }

            index_key = "nyc-taxi/analysis-results/index.json"
            s3_client.put_object(
                Bucket=BUCKET,
                Key=index_key,
                Body=json.dumps(index, indent=2),
                ContentType='application/json'
            )

            print(f"\n{'='*70}")
            print(f"🎉 ANÁLISIS COMPLETADO")
            print(f"{'='*70}")
            print(f"✅ Años analizados: {len(all_results)}")
            print(f"✅ Índice guardado: {index_key}")

    except Exception as e:
        print(f"\n❌ ERROR durante el análisis: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\n✨ Spark Session cerrada correctamente")

    return 0

if __name__ == "__main__":
    sys.exit(main())