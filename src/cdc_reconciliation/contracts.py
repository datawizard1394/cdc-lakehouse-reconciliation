"""Versioned CDC event contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Any

REQUIRED_ENVELOPE = {
    "event_id",
    "entity_id",
    "operation",
    "source_sequence",
    "event_time",
    "schema_version",
    "payload",
}
ALLOWED_PAYLOAD_FIELDS = {
    1: {"name", "email", "country"},
    2: {"name", "email", "country", "loyalty_tier"},
}
REQUIRED_INSERT_FIELDS = {"name", "email", "country"}


def validate_event(event: dict[str, Any]) -> tuple[bool, str]:
    missing = REQUIRED_ENVELOPE - set(event)
    if missing:
        return False, f"missing_envelope_fields:{','.join(sorted(missing))}"
    version = event["schema_version"]
    if version not in ALLOWED_PAYLOAD_FIELDS:
        return False, f"unsupported_schema_version:{version}"
    if event["operation"] not in {"INSERT", "UPDATE", "DELETE"}:
        return False, f"unsupported_operation:{event['operation']}"
    if not isinstance(event["source_sequence"], int) or event["source_sequence"] < 1:
        return False, "invalid_source_sequence"
    try:
        datetime.fromisoformat(str(event["event_time"]).replace("Z", "+00:00"))
    except ValueError:
        return False, "invalid_event_time"
    payload = event["payload"]
    if not isinstance(payload, dict):
        return False, "payload_not_object"
    unexpected = set(payload) - ALLOWED_PAYLOAD_FIELDS[version]
    if unexpected:
        return False, f"unexpected_payload_fields:{','.join(sorted(unexpected))}"
    if event["operation"] == "INSERT" and not REQUIRED_INSERT_FIELDS <= set(payload):
        return False, "insert_missing_required_fields"
    if event["operation"] == "DELETE" and payload:
        return False, "delete_payload_must_be_empty"
    return True, "accepted"

