"""Source-to-target active-state reconciliation."""

from __future__ import annotations

from collections.abc import Iterable

BUSINESS_FIELDS = ("name", "email", "country", "loyalty_tier", "source_sequence")


def reconcile(
    source_rows: Iterable[dict[str, str]],
    target_rows: Iterable[dict[str, str]],
) -> dict[str, object]:
    source = {row["entity_id"]: row for row in source_rows}
    target = {row["entity_id"]: row for row in target_rows if row["is_current"] == "true"}
    missing = sorted(source.keys() - target.keys())
    unexpected = sorted(target.keys() - source.keys())
    mismatches: list[dict[str, str]] = []
    for entity_id in sorted(source.keys() & target.keys()):
        for field in BUSINESS_FIELDS:
            if source[entity_id].get(field, "") != target[entity_id].get(field, ""):
                mismatches.append(
                    {
                        "entity_id": entity_id,
                        "field": field,
                        "source_value": source[entity_id].get(field, ""),
                        "target_value": target[entity_id].get(field, ""),
                    }
                )
    return {
        "passed": not missing and not unexpected and not mismatches,
        "source_active_count": len(source),
        "target_active_count": len(target),
        "missing_in_target": missing,
        "unexpected_in_target": unexpected,
        "value_mismatches": mismatches,
        "difference_count": len(missing) + len(unexpected) + len(mismatches),
    }

