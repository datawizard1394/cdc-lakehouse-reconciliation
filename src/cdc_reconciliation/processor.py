"""Incremental CDC processor with SCD Type 2 state and an event ledger."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from cdc_reconciliation.contracts import validate_event
from cdc_reconciliation.io import (
    atomic_write_csv,
    atomic_write_json,
    read_csv,
    read_jsonl,
    sha256_file,
)
from cdc_reconciliation.reconcile import reconcile

HISTORY_FIELDS = (
    "surrogate_key",
    "entity_id",
    "name",
    "email",
    "country",
    "loyalty_tier",
    "valid_from",
    "valid_to",
    "is_current",
    "source_event_id",
    "source_sequence",
)
LEDGER_FIELDS = (
    "event_id",
    "entity_id",
    "source_sequence",
    "outcome",
    "reason",
)
QUARANTINE_FIELDS = (
    "event_id",
    "entity_id",
    "source_sequence",
    "schema_version",
    "reason",
)


def _surrogate_key(entity_id: str, valid_from: str) -> str:
    return hashlib.sha256(f"{entity_id}|{valid_from}".encode("utf-8")).hexdigest()[:20]


class CDCProcessor:
    """Apply unseen CDC events once and materialize a deterministic SCD2 table."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir

    def run(self, events_path: Path, source_snapshot_path: Path) -> dict[str, object]:
        events = read_jsonl(events_path)
        history_path = self.output_dir / "silver" / "customer_history.csv"
        ledger_path = self.output_dir / "_meta" / "event_ledger.csv"
        quarantine_path = self.output_dir / "quarantine" / "events.csv"
        history = read_csv(history_path)
        ledger = read_csv(ledger_path)
        quarantine = read_csv(quarantine_path)
        seen_ledger = {row["event_id"] for row in ledger}

        unique_batch: dict[str, dict[str, Any]] = {}
        duplicate_delivery_count = 0
        for event in events:
            event_id = str(event.get("event_id", ""))
            if event_id in unique_batch:
                duplicate_delivery_count += 1
            else:
                unique_batch[event_id] = event

        candidates = [
            event for event_id, event in unique_batch.items() if event_id not in seen_ledger
        ]
        skipped_from_ledger = len(unique_batch) - len(candidates)
        candidates.sort(
            key=lambda event: (
                str(event.get("entity_id", "")),
                int(event.get("source_sequence", 0)),
                str(event.get("event_time", "")),
                str(event.get("event_id", "")),
            )
        )

        active = {
            row["entity_id"]: row for row in history if row["is_current"] == "true"
        }
        last_sequence: dict[str, int] = {}
        for row in history:
            last_sequence[row["entity_id"]] = max(
                last_sequence.get(row["entity_id"], 0),
                int(row["source_sequence"]),
            )
        for row in ledger:
            if row["outcome"] == "APPLIED":
                last_sequence[row["entity_id"]] = max(
                    last_sequence.get(row["entity_id"], 0),
                    int(row["source_sequence"]),
                )

        applied = 0
        newly_quarantined = 0
        for event in candidates:
            event_id = str(event.get("event_id", ""))
            entity_id = str(event.get("entity_id", ""))
            sequence = int(event.get("source_sequence", 0))
            valid, reason = validate_event(event)
            if valid and sequence <= last_sequence.get(entity_id, 0):
                valid = False
                reason = "stale_or_replayed_sequence"
            if valid and event["operation"] == "UPDATE" and entity_id not in active:
                valid = False
                reason = "update_without_active_entity"
            if valid and event["operation"] == "INSERT" and entity_id in active:
                valid = False
                reason = "insert_for_active_entity"

            if not valid:
                quarantine.append(
                    {
                        "event_id": event_id,
                        "entity_id": entity_id,
                        "source_sequence": str(sequence),
                        "schema_version": str(event.get("schema_version", "")),
                        "reason": reason,
                    }
                )
                ledger.append(
                    {
                        "event_id": event_id,
                        "entity_id": entity_id,
                        "source_sequence": str(sequence),
                        "outcome": "QUARANTINED",
                        "reason": reason,
                    }
                )
                newly_quarantined += 1
                continue

            previous = active.get(entity_id)
            if previous is not None:
                previous["valid_to"] = str(event["event_time"])
                previous["is_current"] = "false"

            if event["operation"] != "DELETE":
                payload = dict(event["payload"])
                current_values = (
                    {
                        field: previous[field]
                        for field in ("name", "email", "country", "loyalty_tier")
                    }
                    if previous
                    else {"name": "", "email": "", "country": "", "loyalty_tier": ""}
                )
                current_values.update(payload)
                new_row = {
                    "surrogate_key": _surrogate_key(
                        entity_id, str(event["event_time"])
                    ),
                    "entity_id": entity_id,
                    **current_values,
                    "valid_from": str(event["event_time"]),
                    "valid_to": "",
                    "is_current": "true",
                    "source_event_id": event_id,
                    "source_sequence": str(sequence),
                }
                history.append(new_row)
                active[entity_id] = new_row
            else:
                active.pop(entity_id, None)

            last_sequence[entity_id] = sequence
            ledger.append(
                {
                    "event_id": event_id,
                    "entity_id": entity_id,
                    "source_sequence": str(sequence),
                    "outcome": "APPLIED",
                    "reason": "accepted",
                }
            )
            applied += 1

        history.sort(
            key=lambda row: (
                row["entity_id"],
                int(row["source_sequence"]),
                row["valid_from"],
            )
        )
        ledger.sort(key=lambda row: row["event_id"])
        quarantine.sort(key=lambda row: row["event_id"])
        atomic_write_csv(history_path, history, HISTORY_FIELDS)
        atomic_write_csv(ledger_path, ledger, LEDGER_FIELDS)
        atomic_write_csv(quarantine_path, quarantine, QUARANTINE_FIELDS)

        reconciliation = reconcile(read_csv(source_snapshot_path), history)
        atomic_write_json(
            self.output_dir / "_meta" / "reconciliation_report.json",
            reconciliation,
        )
        manifest: dict[str, object] = {
            "pipeline": "synthetic_cdc_lakehouse_reconciliation",
            "synthetic": True,
            "input_checksum": sha256_file(events_path),
            "delivered_events": len(events),
            "unique_events_in_batch": len(unique_batch),
            "duplicate_deliveries_ignored": duplicate_delivery_count,
            "previously_processed_events_skipped": skipped_from_ledger,
            "newly_applied_events": applied,
            "newly_quarantined_events": newly_quarantined,
            "ledger_event_count": len(ledger),
            "history_row_count": len(history),
            "active_entity_count": len(active),
            "reconciliation_passed": reconciliation["passed"],
            "output_checksums": {
                "silver/customer_history.csv": sha256_file(history_path),
                "_meta/event_ledger.csv": sha256_file(ledger_path),
                "quarantine/events.csv": sha256_file(quarantine_path),
            },
        }
        atomic_write_json(self.output_dir / "_meta" / "run_manifest.json", manifest)
        return manifest
