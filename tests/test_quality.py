import pytest
from streaming.quality import validate_event_dict


def test_valid_event():
    event = {
        "event_id": "evt-100",
        "device_id": "MTR-001",
        "event_timestamp": "2026-09-11T10:00:00Z",
        "temperature": 65.5,
        "pressure": 4.2,
        "vibration": 3.1,
        "rpm": 1500,
        "status": "RUNNING"
    }
    is_valid, errors = validate_event_dict(event)
    assert is_valid is True
    assert len(errors) == 0


def test_missing_device_id():
    event = {
        "event_id": "evt-101",
        "device_id": "",
        "event_timestamp": "2026-09-11T10:00:00Z",
        "temperature": 65.5,
        "pressure": 4.2,
        "vibration": 3.1,
        "rpm": 1500,
        "status": "RUNNING"
    }
    is_valid, errors = validate_event_dict(event)
    assert is_valid is False
    assert "device_id_null" in errors


def test_invalid_temperature():
    event = {
        "event_id": "evt-102",
        "device_id": "MTR-002",
        "event_timestamp": "2026-09-11T10:00:00Z",
        "temperature": -999.0,
        "pressure": 4.2,
        "vibration": 3.1,
        "rpm": 1500,
        "status": "RUNNING"
    }
    is_valid, errors = validate_event_dict(event)
    assert is_valid is False
    assert "temperature_out_of_range" in errors


def test_negative_pressure():
    event = {
        "event_id": "evt-103",
        "device_id": "MTR-003",
        "event_timestamp": "2026-09-11T10:00:00Z",
        "temperature": 60.0,
        "pressure": -5.0,
        "vibration": 3.1,
        "rpm": 1500,
        "status": "RUNNING"
    }
    is_valid, errors = validate_event_dict(event)
    assert is_valid is False
    assert "pressure_negative" in errors


def test_invalid_status():
    event = {
        "event_id": "evt-104",
        "device_id": "MTR-004",
        "event_timestamp": "2026-09-11T10:00:00Z",
        "temperature": 60.0,
        "pressure": 4.0,
        "vibration": 3.1,
        "rpm": 1500,
        "status": "EXPLODED"
    }
    is_valid, errors = validate_event_dict(event)
    assert is_valid is False
    assert "invalid_status" in errors
