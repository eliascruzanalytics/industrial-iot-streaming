import os
import shutil
import tempfile
import pytest
from consumers.alert_consumer import AlertConsumer
from consumers.metrics_consumer import MetricsConsumer
from consumers.archive_consumer import ArchiveConsumer
from consumers.dlq_consumer import DlqConsumer
from consumers.maintenance_consumer import MaintenanceConsumer


@pytest.fixture
def temp_data_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_alert_consumer_critical(temp_data_dir):
    consumer = AlertConsumer(output_dir=temp_data_dir)
    critical_event = {
        "event_id": "evt-crit-001",
        "device_id": "MTR-001",
        "event_timestamp": "2026-09-13T18:00:00Z",
        "temperature": 96.5,
        "pressure": 4.5,
        "vibration": 5.0,
        "status": "RUNNING",
        "anomaly_level": "CRITICAL"
    }
    alert = consumer.process_event(critical_event)
    assert alert is not None
    assert alert["device_id"] == "MTR-001"
    assert alert["severity"] == "CRITICAL"
    assert len(consumer.alerts_history) == 1
    assert os.path.exists(os.path.join(temp_data_dir, "alerts.jsonl"))


def test_alert_consumer_normal(temp_data_dir):
    consumer = AlertConsumer(output_dir=temp_data_dir)
    normal_event = {
        "event_id": "evt-norm-001",
        "device_id": "MTR-002",
        "event_timestamp": "2026-09-13T18:00:00Z",
        "temperature": 60.0,
        "pressure": 3.0,
        "vibration": 2.0,
        "status": "RUNNING",
        "anomaly_level": "NORMAL"
    }
    alert = consumer.process_event(normal_event)
    assert alert is None
    assert len(consumer.alerts_history) == 0


def test_metrics_consumer_aggregation(temp_data_dir):
    consumer = MetricsConsumer(output_dir=temp_data_dir)
    event1 = {
        "event_id": "evt-m-001",
        "device_id": "MTR-001",
        "temperature": 80.0,
        "pressure": 4.0,
        "vibration": 5.0,
        "status": "RUNNING"
    }
    event2 = {
        "event_id": "evt-m-002",
        "device_id": "MTR-001",
        "temperature": 100.0,
        "pressure": 6.0,
        "vibration": 15.0,
        "status": "RUNNING"
    }

    consumer.process_event(event1)
    summary = consumer.process_event(event2)

    assert summary["total_events"] == 2
    assert summary["avg_temperature"] == 90.0
    assert summary["min_temperature"] == 80.0
    assert summary["max_temperature"] == 100.0
    assert summary["avg_vibration"] == 10.0
    assert os.path.exists(os.path.join(temp_data_dir, "operational_metrics.json"))


def test_archive_consumer(temp_data_dir):
    consumer = ArchiveConsumer(base_dir=temp_data_dir)
    event = {
        "event_id": "evt-arch-001",
        "device_id": "MTR-003",
        "event_timestamp": "2026-09-13T18:30:00Z",
        "temperature": 70.0
    }
    filepath = consumer.process_event(event)
    assert os.path.exists(filepath)
    assert consumer.archived_count == 1


def test_dlq_consumer(temp_data_dir):
    consumer = DlqConsumer(quarantine_dir=temp_data_dir, error_threshold_alert=2)
    bad_record = {
        "event_id": "evt-bad-001",
        "device_id": "MTR-004",
        "error_reason": "temperature_out_of_range"
    }
    res = consumer.process_quarantine_record(bad_record)
    assert res["error_reason"] == "temperature_out_of_range"
    assert consumer.error_counts_by_reason["temperature_out_of_range"] == 1
    assert os.path.exists(os.path.join(temp_data_dir, "dlq_summary.json"))


def test_maintenance_consumer(temp_data_dir):
    consumer = MaintenanceConsumer(
        output_dir=temp_data_dir,
        max_accumulated_vibration=10.0
    )
    high_vib_event = {
        "event_id": "evt-maint-001",
        "device_id": "MTR-005",
        "temperature": 70.0,
        "pressure": 3.0,
        "vibration": 12.0,
        "status": "RUNNING"
    }
    res = consumer.process_event(high_vib_event)
    assert res["maintenance_required"] is True
    assert len(consumer.work_orders) == 1
    assert os.path.exists(os.path.join(temp_data_dir, "equipment_lifecycle.json"))
