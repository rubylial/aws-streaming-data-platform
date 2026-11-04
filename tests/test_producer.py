#!/usr/bin/env python3
"""
Tests unitarios para el productor de eventos Kinesis.

Ejecutar:
  python -m pytest tests/test_producer.py -v

No requiere AWS ni boto3 real — todo está mockeado.
"""

import json
import sys
import unittest
from unittest.mock import MagicMock, patch

# Agregar scripts/producer al path
sys.path.insert(0, "scripts/producer")

from producer import generate_event, send_to_kinesis, MERCHANTS, STATUSES, CURRENCY


class TestGenerateEvent(unittest.TestCase):
    """Tests para la generación de eventos sintéticos."""

    def test_event_has_required_fields(self):
        """El evento debe tener los 6 campos del contrato."""
        event = generate_event()
        required = {"event_id", "event_time", "merchant_id", "amount", "currency", "status"}
        self.assertEqual(set(event.keys()), required)

    def test_event_id_format(self):
        """event_id debe empezar con 'evt-'."""
        event = generate_event()
        self.assertTrue(event["event_id"].startswith("evt-"))

    def test_event_id_unique(self):
        """Dos eventos deben tener event_id distintos."""
        e1 = generate_event()
        e2 = generate_event()
        self.assertNotEqual(e1["event_id"], e2["event_id"])

    def test_event_time_format(self):
        """event_time debe ser ISO 8601 UTC."""
        event = generate_event()
        self.assertTrue(event["event_time"].endswith("Z"))
        self.assertRegex(event["event_time"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_merchant_id_valid(self):
        """merchant_id debe ser uno de los predefinidos."""
        event = generate_event()
        self.assertIn(event["merchant_id"], MERCHANTS)

    def test_amount_positive(self):
        """amount debe ser positivo."""
        event = generate_event()
        self.assertGreater(event["amount"], 0)

    def test_currency_is_cop(self):
        """currency debe ser COP."""
        event = generate_event()
        self.assertEqual(event["currency"], CURRENCY)

    def test_status_valid(self):
        """status debe ser uno de los predefinidos."""
        event = generate_event()
        self.assertIn(event["status"], STATUSES)

    def test_event_serializable(self):
        """El evento debe ser serializable a JSON."""
        event = generate_event()
        serialized = json.dumps(event)
        deserialized = json.loads(serialized)
        self.assertEqual(event, deserialized)


class TestSendToKinesis(unittest.TestCase):
    """Tests para el envío a Kinesis con boto3 mockeado."""

    @patch("producer.boto3")
    def test_put_records_called_with_correct_params(self, mock_boto3):
        """PutRecords debe llamarse con StreamName y Records correctos."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"ShardId": "shardId-000000000000", "SequenceNumber": "1"}],
        }

        events = [generate_event()]
        send_to_kinesis("test-stream", events, "us-east-1")

        mock_boto3.client.assert_called_once_with("kinesis", region_name="us-east-1")
        mock_client.put_records.assert_called_once()

        call_args = mock_client.put_records.call_args
        self.assertEqual(call_args.kwargs["StreamName"], "test-stream")
        self.assertEqual(len(call_args.kwargs["Records"]), 1)

    @patch("producer.boto3")
    def test_partition_key_is_merchant_id(self, mock_boto3):
        """PartitionKey debe ser merchant_id del evento."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"ShardId": "shardId-000000000000", "SequenceNumber": "1"}],
        }

        event = generate_event()
        send_to_kinesis("test-stream", [event], "us-east-1")

        records = mock_client.put_records.call_args.kwargs["Records"]
        self.assertEqual(records[0]["PartitionKey"], event["merchant_id"])

    @patch("producer.boto3")
    def test_data_is_valid_json(self, mock_boto3):
        """Data en cada record debe ser JSON válido del evento."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"ShardId": "shardId-000000000000", "SequenceNumber": "1"}],
        }

        event = generate_event()
        send_to_kinesis("test-stream", [event], "us-east-1")

        records = mock_client.put_records.call_args.kwargs["Records"]
        decoded = json.loads(records[0]["Data"].decode("utf-8"))
        self.assertEqual(decoded["event_id"], event["event_id"])

    @patch("producer.boto3")
    def test_multiple_events_batch(self, mock_boto3):
        """Múltiples eventos deben enviarse en un solo PutRecords."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [
                {"ShardId": "shardId-000000000000", "SequenceNumber": str(i)}
                for i in range(5)
            ],
        }

        events = [generate_event() for _ in range(5)]
        send_to_kinesis("test-stream", events, "us-east-1")

        records = mock_client.put_records.call_args.kwargs["Records"]
        self.assertEqual(len(records), 5)

    @patch("producer.boto3")
    def test_response_returns_failed_count(self, mock_boto3):
        """La respuesta debe incluir FailedRecordCount."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.put_records.return_value = {
            "FailedRecordCount": 2,
            "Records": [
                {"ShardId": "shardId-000000000000", "SequenceNumber": "1"},
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded"},
                {"ErrorCode": "InternalFailure", "ErrorMessage": "Internal error"},
            ],
        }

        response = send_to_kinesis("test-stream", [generate_event()] * 3, "us-east-1")
        self.assertEqual(response["FailedRecordCount"], 2)


class TestDuplicateDetection(unittest.TestCase):
    """Tests para verificar que event_id permite dedupe."""

    def test_sample_events_contain_duplicate(self):
        """sample_events.json debe contener un duplicado intencional."""
        with open("scripts/producer/sample_events.json", "r") as f:
            events = json.load(f)

        event_ids = [e["event_id"] for e in events]
        unique_ids = set(event_ids)
        self.assertGreater(len(event_ids), len(unique_ids),
                           "sample_events.json debe contener al menos un event_id duplicado")


if __name__ == "__main__":
    unittest.main()
