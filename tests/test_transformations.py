import pytest
from streaming.refined import calculate_anomaly_python


def test_normal_machine_anomaly():
    # temperature <= 75, pressure <= 5, vibration <= 6
    level = calculate_anomaly_python(temperature=60.0, pressure=4.0, vibration=3.0)
    assert level == "NORMAL"


def test_warning_machine_anomaly():
    # temperature > 75 or pressure > 5 or vibration > 6, but none exceed critical thresholds
    level_temp = calculate_anomaly_python(temperature=80.0, pressure=4.0, vibration=3.0)
    assert level_temp == "WARNING"

    level_press = calculate_anomaly_python(temperature=60.0, pressure=6.0, vibration=3.0)
    assert level_press == "WARNING"

    level_vib = calculate_anomaly_python(temperature=60.0, pressure=4.0, vibration=8.0)
    assert level_vib == "WARNING"


def test_critical_machine_temperature_anomaly():
    # temperature > 90
    level = calculate_anomaly_python(temperature=92.5, pressure=4.0, vibration=3.0)
    assert level == "CRITICAL"


def test_critical_machine_vibration_anomaly():
    # vibration > 15
    level = calculate_anomaly_python(temperature=60.0, pressure=4.0, vibration=18.0)
    assert level == "CRITICAL"


def test_critical_machine_pressure_anomaly():
    # pressure > 7
    level = calculate_anomaly_python(temperature=60.0, pressure=8.5, vibration=3.0)
    assert level == "CRITICAL"
