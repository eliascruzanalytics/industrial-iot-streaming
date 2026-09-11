import pytest
from typing import List, Dict, Any


def deduplicate_events_python(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicates a list of event dictionaries by event_id keeping the first occurrence."""
    seen = set()
    deduped = []
    for evt in events:
        eid = evt.get("event_id")
        if eid not in seen:
            seen.add(eid)
            deduped.append(evt)
    return deduped


def test_duplicate_event_removal():
    raw_events = [
        {"event_id": "evt-001", "device_id": "MTR-001", "temperature": 60.0},
        {"event_id": "evt-002", "device_id": "MTR-002", "temperature": 65.0},
        {"event_id": "evt-001", "device_id": "MTR-001", "temperature": 60.0},  # Duplicate
        {"event_id": "evt-003", "device_id": "MTR-003", "temperature": 70.0},
    ]

    deduped = deduplicate_events_python(raw_events)
    assert len(deduped) == 3
    event_ids = [e["event_id"] for e in deduped]
    assert event_ids == ["evt-001", "evt-002", "evt-003"]
