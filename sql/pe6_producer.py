#!/usr/bin/env python3
"""PE6 — Productor de eventos de prueba para Kinesis."""

import json
import time
import uuid
import random
import boto3
from datetime import datetime, timezone

STREAM_NAME = "streaming-data-platform-events-dev"
REGION = "us-east-1"
NUM_EVENTS = 50

MERCHANTS = ["merchant_001", "merchant_002", "merchant_003", "merchant_004", "merchant_005"]
STATUSES = ["approved", "declined", "pending", "refunded"]
CURRENCIES = ["USD", "COP", "EUR"]


def generate_event():
    merchant_id = random.choice(MERCHANTS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "merchant_id": merchant_id,
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
    }


def main():
    kinesis = boto3.client("kinesis", region_name=REGION)

    print(f"Enviando {NUM_EVENTS} eventos a {STREAM_NAME}...")
    success = 0
    errors = 0

    for i in range(NUM_EVENTS):
        event = generate_event()
        try:
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=event["merchant_id"],
            )
            shard = response["ShardId"]
            seq = response["SequenceNumber"][:20]
            print(f"  [{i+1}/{NUM_EVENTS}] {event['merchant_id']} ${event['amount']:.2f} -> {shard} (seq: {seq}...)")
            success += 1
        except Exception as e:
            print(f"  [{i+1}/{NUM_EVENTS}] ERROR: {e}")
            errors += 1
        time.sleep(0.2)

    print(f"\nResultado: {success} enviados, {errors} errores")


if __name__ == "__main__":
    main()
