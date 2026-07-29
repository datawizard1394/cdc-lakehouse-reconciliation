"""Deterministic synthetic customer CDC event generator."""

from __future__ import annotations

import hashlib
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cdc_reconciliation.io import (
    atomic_write_csv,
    atomic_write_json,
    atomic_write_jsonl,
    sha256_file,
)


@dataclass(frozen=True)
class CDCConfig:
    seed: int = 20260728
    entities: int = 24
    duplicate_every: int = 5
    unsupported_events: int = 2

    def validate(self) -> None:
        if self.entities < 1:
            raise ValueError("entities must be positive")
        if self.duplicate_every < 1:
            raise ValueError("duplicate_every must be positive")
        if self.unsupported_events < 0:
            raise ValueError("unsupported_events cannot be negative")


FIRST_NAMES = ("Avery", "Jordan", "Morgan", "Riley", "Taylor", "Casey")
LAST_NAMES = ("Chen", "Patel", "Martin", "Kim", "Singh", "Nguyen")
COUNTRIES = ("CA", "US", "GB")
TIERS = ("bronze", "silver", "gold")


def _event_id(entity_id: str, sequence: int, operation: str) -> str:
    material = f"{entity_id}|{sequence}|{operation}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _event(
    entity_id: str,
    sequence: int,
    operation: str,
    event_time: datetime,
    schema_version: int,
    payload: dict[str, str],
) -> dict[str, Any]:
    return {
        "event_id": _event_id(entity_id, sequence, operation),
        "entity_id": entity_id,
        "operation": operation,
        "source_sequence": sequence,
        "event_time": event_time.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "schema_version": schema_version,
        "payload": payload,
    }


def generate_cdc_dataset(output_dir: Path, config: CDCConfig) -> dict[str, Any]:
    """Create shuffled CDC deliveries plus an authoritative active snapshot.

    Valid schema versions are v1 (name, email, country) and backward-compatible
    v2 (adds loyalty_tier). A small number of v99 records is intentionally
    included to demonstrate quarantine and is excluded from the source snapshot.
    """
    config.validate()
    rng = random.Random(config.seed)
    base_time = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    logical_events: list[dict[str, Any]] = []
    authoritative: dict[str, dict[str, str]] = {}

    for index in range(1, config.entities + 1):
        entity_id = f"C{index:05d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        inserted = {
            "name": f"{first} {last}",
            "email": f"{first}.{last}.{index}@example.test".lower(),
            "country": rng.choice(COUNTRIES),
        }
        logical_events.append(
            _event(
                entity_id,
                1,
                "INSERT",
                base_time + timedelta(minutes=index),
                1,
                inserted,
            )
        )
        authoritative[entity_id] = {
            "entity_id": entity_id,
            **inserted,
            "loyalty_tier": "",
            "source_sequence": "1",
        }

        if index % 2 == 0:
            update = {
                "email": f"{first}.{last}.{index}.updated@example.test".lower(),
                "loyalty_tier": rng.choice(TIERS),
            }
            logical_events.append(
                _event(
                    entity_id,
                    2,
                    "UPDATE",
                    base_time + timedelta(days=1, minutes=index),
                    2,
                    update,
                )
            )
            authoritative[entity_id].update(update)
            authoritative[entity_id]["source_sequence"] = "2"

        if index % 11 == 0:
            next_sequence = int(authoritative[entity_id]["source_sequence"]) + 1
            logical_events.append(
                _event(
                    entity_id,
                    next_sequence,
                    "DELETE",
                    base_time + timedelta(days=2, minutes=index),
                    2,
                    {},
                )
            )
            authoritative.pop(entity_id)

    invalid_events = [
        _event(
            f"Q{index:05d}",
            1,
            "INSERT",
            base_time + timedelta(days=3, minutes=index),
            99,
            {"name": "Unsupported Schema", "email": "quarantine@example.test"},
        )
        for index in range(1, config.unsupported_events + 1)
    ]

    deliveries = [dict(event) for event in logical_events]
    deliveries.extend(
        dict(event)
        for position, event in enumerate(logical_events, start=1)
        if position % config.duplicate_every == 0
    )
    deliveries.extend(invalid_events)
    rng.shuffle(deliveries)

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(output_dir / "events.jsonl", deliveries)
    snapshot_rows = [authoritative[key] for key in sorted(authoritative)]
    atomic_write_csv(
        output_dir / "source_snapshot.csv",
        snapshot_rows,
        (
            "entity_id",
            "name",
            "email",
            "country",
            "loyalty_tier",
            "source_sequence",
        ),
    )
    manifest = {
        "dataset": "synthetic_customer_cdc_demo",
        "synthetic": True,
        "config": asdict(config),
        "logical_valid_events": len(logical_events),
        "delivered_events": len(deliveries),
        "intentional_duplicate_deliveries": len(deliveries)
        - len(logical_events)
        - len(invalid_events),
        "intentional_unsupported_schema_events": len(invalid_events),
        "expected_active_entities": len(snapshot_rows),
        "checksums": {
            "events.jsonl": sha256_file(output_dir / "events.jsonl"),
            "source_snapshot.csv": sha256_file(output_dir / "source_snapshot.csv"),
        },
    }
    atomic_write_json(output_dir / "generator_manifest.json", manifest)
    return manifest
