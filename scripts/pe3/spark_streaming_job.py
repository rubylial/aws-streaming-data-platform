#!/usr/bin/env python3
"""
Spark Structured Streaming — Sensores Urbanos.

PE3 (25%): consume topic urban_sensors desde Kafka,
aplica ventana tumbling de 1 minuto y calcula promedio
de temperature y air_quality_index por sensor_id.

Ejecución local:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    scripts/pe3/spark_streaming_job.py

Variables de entorno:
  KAFKA_BOOTSTRAP_SERVERS  (default: localhost:9092)
  KAFKA_TOPIC              (default: urban_sensors)
  OUTPUT_MODE              (default: console)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    round as spark_round,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# --- Configuración ---

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")
OUTPUT_MODE = os.getenv("OUTPUT_MODE", "console")

# --- Esquema del evento de sensor ---

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=False),
    StructField("humidity", DoubleType(), nullable=False),
    StructField("air_quality_index", DoubleType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
])


def create_spark_session() -> SparkSession:
    """Crea SparkSession con configuración para streaming."""
    return (
        SparkSession.builder
        .appName("PE3-UrbanSensors-Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"=== Spark Structured Streaming ===")
    print(f"=== Kafka: {BOOTSTRAP_SERVERS} ===")
    print(f"=== Topic: {TOPIC} ===\n")

    # 1. Leer stream desde Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parsear JSON y extraer campos
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data"))
        .select(
            col("data.sensor_id"),
            col("data.temperature"),
            col("data.humidity"),
            col("data.air_quality_index"),
            col("data.timestamp").cast(TimestampType()).alias("event_time"),
        )
    )

    # 3. Ventana tumbling de 1 minuto: promedio por sensor
    windowed_avg = (
        parsed
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("sensor_id"),
        )
        .agg(
            spark_round(avg("temperature"), 2).alias("avg_temperature"),
            spark_round(avg("air_quality_index"), 1).alias("avg_aqi"),
            spark_round(avg("humidity"), 1).alias("avg_humidity"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("avg_temperature"),
            col("avg_aqi"),
            col("avg_humidity"),
        )
    )

    # 4. Escribir resultados
    if OUTPUT_MODE == "console":
        query = (
            windowed_avg.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "50")
            .trigger(processingTime="30 seconds")
            .start()
        )
    elif OUTPUT_MODE == "parquet":
        output_path = os.getenv("OUTPUT_PATH", "/tmp/pe3-output/sensor_averages")
        checkpoint_path = os.getenv("CHECKPOINT_PATH", "/tmp/pe3-checkpoints")
        query = (
            windowed_avg.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="1 minute")
            .start()
        )
    else:
        print(f"OUTPUT_MODE no soportado: {OUTPUT_MODE}")
        spark.stop()
        return

    print("Streaming iniciado. Ctrl+C para detener.\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
