#!/usr/bin/env python3
"""
Productor de datos de sensores urbanos para Kafka.

PE3: genera eventos JSON al topic urban_sensors con 3+ particiones.

Esquema:
  sensor_id, temperature, humidity, air_quality_index, timestamp

Modos:
  --local        Imprime eventos a stdout (sin Kafka)
  --bootstrap-servers HOST:PORT  Envía a Kafka

El sensor_id se usa como key para distribución entre particiones.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from random import choice, gauss, uniform


# --- Configuración de sensores simulados ---

SENSORS = [
    {"id": "sensor-norte-001", "base_temp": 22.0, "base_aqi": 45},
    {"id": "sensor-centro-002", "base_temp": 24.0, "base_aqi": 72},
    {"id": "sensor-sur-003", "base_temp": 21.0, "base_aqi": 55},
    {"id": "sensor-oriente-004", "base_temp": 23.0, "base_aqi": 60},
    {"id": "sensor-occidente-005", "base_temp": 20.0, "base_aqi": 38},
]


def generate_sensor_event() -> dict:
    """Genera un evento de sensor urbano con variación realista."""
    sensor = choice(SENSORS)
    return {
        "sensor_id": sensor["id"],
        "temperature": round(gauss(sensor["base_temp"], 2.0), 2),
        "humidity": round(uniform(40.0, 95.0), 2),
        "air_quality_index": max(0, round(gauss(sensor["base_aqi"], 10.0), 1)),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def run_local(count: int, interval: float) -> None:
    """Modo local: imprime eventos a stdout sin Kafka."""
    print("=== MODO LOCAL — sensores urbanos ===")
    print(f"=== Generando {count} eventos ===\n")

    for i in range(count):
        event = generate_sensor_event()
        print(json.dumps(event, indent=2))
        if i < count - 1 and interval > 0:
            time.sleep(interval)

    print(f"\n=== {count} eventos generados (LOCAL) ===")


def run_kafka(bootstrap_servers: str, topic: str, count: int, interval: float) -> None:
    """Envía eventos a Kafka usando kafka-python."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("ERROR: kafka-python no instalado. Ejecuta: pip install kafka-python",
              file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"=== Enviando {count} eventos a {topic} ===")
    print(f"=== Kafka: {bootstrap_servers} ===\n")

    sent = 0
    errors = 0

    for i in range(count):
        event = generate_sensor_event()
        try:
            future = producer.send(
                topic,
                key=event["sensor_id"],
                value=event,
            )
            future.get(timeout=10)
            sent += 1
            print(f"[{sent}] {event['sensor_id']} -> "
                  f"temp={event['temperature']}, aqi={event['air_quality_index']}")
        except Exception as e:
            errors += 1
            print(f"ERROR enviando evento {i}: {e}", file=sys.stderr)

        if i < count - 1 and interval > 0:
            time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"\n=== Enviados: {sent}, Errores: {errors} ===")


def main():
    parser = argparse.ArgumentParser(
        description="Productor de sensores urbanos para Kafka"
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--local",
        action="store_true",
        help="Modo local: stdout sin Kafka"
    )
    mode.add_argument(
        "--bootstrap-servers",
        type=str,
        metavar="HOST:PORT",
        help="Kafka bootstrap servers"
    )

    parser.add_argument(
        "--topic", type=str, default="urban_sensors",
        help="Nombre del topic Kafka (default: urban_sensors)"
    )
    parser.add_argument(
        "--count", type=int, default=100,
        help="Número de eventos (default: 100, 0=infinito)"
    )
    parser.add_argument(
        "--interval", type=float, default=1.0,
        help="Segundos entre eventos (default: 1.0)"
    )

    args = parser.parse_args()

    if args.local:
        run_local(args.count, args.interval)
    else:
        if args.count == 0:
            print("Modo continuo (Ctrl+C para detener)")
            i = 0
            try:
                while True:
                    run_kafka(args.bootstrap_servers, args.topic, 1, 0)
                    i += 1
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                print(f"\nDetenido tras {i} eventos")
        else:
            run_kafka(args.bootstrap_servers, args.topic, args.count, args.interval)


if __name__ == "__main__":
    main()
