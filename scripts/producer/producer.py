#!/usr/bin/env python3
"""
Productor de eventos sintéticos para Kinesis Data Streams.

Requisito PE2 (30%): productor JSON con PartitionKey variable.

Modos de ejecución:
  --local       Imprime eventos a stdout (sin AWS, sin costo)
  --stream NAME Envía eventos a Kinesis Data Stream real

Contrato de evento:
  event_id, event_time (UTC), merchant_id, amount, currency, status

PartitionKey: merchant_id (distribuye shards por comercio)

BLOQUEADO_POR_COSTO: no ejecutar con --stream sin aprobación.
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from random import choice, uniform

try:
    import boto3
except ImportError:
    boto3 = None


# --- Constantes del contrato de datos ---

MERCHANTS = [
    "merchant-001",
    "merchant-002",
    "merchant-003",
    "merchant-004",
    "merchant-005",
]

STATUSES = ["completed", "pending", "failed", "refunded"]

CURRENCY = "COP"


def generate_event() -> dict:
    """Genera un evento sintético conforme al contrato JSON."""
    return {
        "event_id": f"evt-{uuid.uuid4().hex[:12]}",
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "merchant_id": choice(MERCHANTS),
        "amount": round(uniform(5000.0, 500000.0), 2),
        "currency": CURRENCY,
        "status": choice(STATUSES),
    }


def send_to_kinesis(stream_name: str, events: list[dict], region: str) -> dict:
    """Envía eventos a Kinesis usando PutRecords (batch).

    PartitionKey = merchant_id para distribución por comercio.

    Returns:
        Respuesta de PutRecords con FailedRecordCount.
    """
    if boto3 is None:
        print("ERROR: boto3 no instalado. Ejecuta: pip install boto3", file=sys.stderr)
        sys.exit(1)

    client = boto3.client("kinesis", region_name=region)

    records = [
        {
            "Data": json.dumps(event).encode("utf-8"),
            "PartitionKey": event["merchant_id"],
        }
        for event in events
    ]

    response = client.put_records(StreamName=stream_name, Records=records)
    return response


def send_from_file(stream_name: str, filepath: str, region: str) -> None:
    """Envía eventos desde un archivo JSON a Kinesis."""
    with open(filepath, "r", encoding="utf-8") as f:
        events = json.load(f)

    print(f"Enviando {len(events)} eventos desde {filepath}...")
    response = send_to_kinesis(stream_name, events, region)
    failed = response.get("FailedRecordCount", 0)
    print(f"Enviados: {len(events)}, Fallidos: {failed}")

    if failed > 0:
        for i, record in enumerate(response["Records"]):
            if "ErrorCode" in record:
                print(f"  Record {i}: {record['ErrorCode']} - {record.get('ErrorMessage', '')}")


def run_local(count: int, interval: float) -> None:
    """Modo local: genera e imprime eventos a stdout sin AWS."""
    print(f"=== MODO LOCAL — generando {count} eventos ===")
    print(f"=== Sin conexión a AWS, sin costo ===\n")

    for i in range(count):
        event = generate_event()
        print(json.dumps(event, indent=2))
        if i < count - 1 and interval > 0:
            time.sleep(interval)

    print(f"\n=== {count} eventos generados (LOCAL) ===")


def run_stream(stream_name: str, count: int, batch_size: int, region: str) -> None:
    """Modo stream: envía eventos a Kinesis en batches."""
    print(f"=== MODO STREAM — enviando a {stream_name} ===")
    print(f"=== ADVERTENCIA: esto genera cargos AWS ===\n")

    total_sent = 0
    total_failed = 0

    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        batch = [generate_event() for _ in range(batch_end - batch_start)]

        response = send_to_kinesis(stream_name, batch, region)
        failed = response.get("FailedRecordCount", 0)
        sent = len(batch) - failed

        total_sent += sent
        total_failed += failed

        print(f"Batch {batch_start // batch_size + 1}: "
              f"enviados={sent}, fallidos={failed}")

    print(f"\n=== Total: enviados={total_sent}, fallidos={total_failed} ===")


def main():
    parser = argparse.ArgumentParser(
        description="Productor de eventos sintéticos para Kinesis"
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--local",
        action="store_true",
        help="Modo local: imprime eventos a stdout (sin AWS)"
    )
    mode.add_argument(
        "--stream",
        type=str,
        metavar="NAME",
        help="Nombre del Kinesis Data Stream destino"
    )
    mode.add_argument(
        "--file",
        nargs=2,
        metavar=("STREAM", "FILEPATH"),
        help="Envía eventos desde archivo JSON al stream"
    )

    parser.add_argument(
        "--count", type=int, default=10,
        help="Número de eventos a generar (default: 10)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=25,
        help="Eventos por PutRecords batch (default: 25, max: 500)"
    )
    parser.add_argument(
        "--interval", type=float, default=0.5,
        help="Segundos entre eventos en modo local (default: 0.5)"
    )
    parser.add_argument(
        "--region", type=str, default="us-east-1",
        help="Región AWS (default: us-east-1)"
    )

    args = parser.parse_args()

    if args.local:
        run_local(args.count, args.interval)
    elif args.stream:
        run_stream(args.stream, args.count, args.batch_size, args.region)
    elif args.file:
        send_from_file(args.file[0], args.file[1], args.region)


if __name__ == "__main__":
    main()