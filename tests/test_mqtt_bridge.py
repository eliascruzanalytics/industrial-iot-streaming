import json
from unittest.mock import MagicMock, patch
import pytest

from bridge.mqtt_to_kafka import (
    validate_json_payload,
    MqttToKafkaBridge,
)


def test_valid_json_payload_acceptance():
    valid_raw = json.dumps({
        "event_id": "evt-001",
        "device_id": "MTR-001",
        "temperature": 75.0,
        "status": "RUNNING"
    })
    result = validate_json_payload(valid_raw)
    assert result is not None
    assert result["event_id"] == "evt-001"
    assert result["device_id"] == "MTR-001"


def test_invalid_json_payload_handling():
    invalid_raw = "THIS IS NOT VALID JSON {"
    result = validate_json_payload(invalid_raw)
    assert result is None


def test_invalid_non_dict_json_payload():
    invalid_array_raw = json.dumps(["evt-001", "MTR-001"])
    result = validate_json_payload(invalid_array_raw)
    assert result is None


def test_mqtt_message_results_in_kafka_publication():
    mock_kafka_producer = MagicMock()
    mock_future = MagicMock()
    mock_meta = MagicMock()
    mock_meta.topic = "iot-machine-events"
    mock_meta.partition = 0
    mock_meta.offset = 12
    mock_future.get.return_value = mock_meta
    mock_kafka_producer.send.return_value = mock_future

    bridge = MqttToKafkaBridge(
        mqtt_host="localhost",
        mqtt_port=1883,
        kafka_bootstrap="localhost:9092",
        kafka_topic="iot-machine-events",
        kafka_producer=mock_kafka_producer
    )

    valid_payload = json.dumps({
        "event_id": "evt-001928",
        "device_id": "MTR-003",
        "event_timestamp": "2026-09-11T10:46:31Z",
        "temperature": 87.3,
        "pressure": 4.8,
        "vibration": 12.4,
        "rpm": 1740,
        "status": "RUNNING"
    })

    success = bridge.process_message(
        raw_topic="industrial/machines/MTR-003/telemetry",
        raw_payload=valid_payload
    )

    assert success is True
    assert mock_kafka_producer.send.called
    call_args = mock_kafka_producer.send.call_args
    
    # Verify topic, message key (device_id), and target payload
    assert call_args.kwargs["topic"] == "iot-machine-events"
    assert call_args.kwargs["key"] == "MTR-003"
    assert call_args.kwargs["value"]["event_id"] == "evt-001928"


def test_kafka_message_key_uses_device_id():
    mock_kafka_producer = MagicMock()
    mock_future = MagicMock()
    mock_meta = MagicMock()
    mock_future.get.return_value = mock_meta
    mock_kafka_producer.send.return_value = mock_future

    bridge = MqttToKafkaBridge(
        kafka_topic="iot-machine-events",
        kafka_producer=mock_kafka_producer
    )

    valid_payload = json.dumps({
        "event_id": "evt-555",
        "device_id": "MTR-005",
        "temperature": 70.0,
        "status": "RUNNING"
    })

    bridge.process_message("industrial/machines/MTR-005/telemetry", valid_payload)
    call_args = mock_kafka_producer.send.call_args
    assert call_args.kwargs["key"] == "MTR-005"


def test_target_kafka_topic_assertion():
    mock_kafka_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock()
    mock_kafka_producer.send.return_value = mock_future

    target_topic = "iot-machine-events"
    bridge = MqttToKafkaBridge(
        kafka_topic=target_topic,
        kafka_producer=mock_kafka_producer
    )

    payload = json.dumps({"event_id": "evt-1", "device_id": "MTR-001"})
    bridge.process_message("industrial/machines/MTR-001/telemetry", payload)

    assert mock_kafka_producer.send.call_args.kwargs["topic"] == target_topic


def test_bridge_reconnection_and_error_handling():
    mock_client = MagicMock()
    with patch("paho.mqtt.client.Client", return_value=mock_client):
        bridge = MqttToKafkaBridge(mqtt_host="invalid_host", mqtt_port=1883)
        assert bridge.is_connected is False

        # Simulate on_connect failure callback
        bridge.on_mqtt_connect(mock_client, None, None, rc=5)
        assert bridge.is_connected is False

        # Simulate on_connect success callback
        bridge.on_mqtt_connect(mock_client, None, None, rc=0)
        assert bridge.is_connected is True
        assert mock_client.subscribe.called
