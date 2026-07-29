from __future__ import annotations

import unittest

from cdc_reconciliation.contracts import validate_event
from cdc_reconciliation.reconcile import reconcile


class ContractAndReconciliationTests(unittest.TestCase):
    def test_unexpected_v1_field_is_rejected(self) -> None:
        valid, reason = validate_event(
            {
                "event_id": "event-1",
                "entity_id": "C00001",
                "operation": "INSERT",
                "source_sequence": 1,
                "event_time": "2025-01-01T00:00:00Z",
                "schema_version": 1,
                "payload": {
                    "name": "Synthetic Person",
                    "email": "person@example.test",
                    "country": "CA",
                    "loyalty_tier": "gold",
                },
            }
        )
        self.assertFalse(valid)
        self.assertEqual(reason, "unexpected_payload_fields:loyalty_tier")

    def test_reconciliation_explains_field_mismatch(self) -> None:
        source = [
            {
                "entity_id": "C1",
                "name": "Synthetic Person",
                "email": "source@example.test",
                "country": "CA",
                "loyalty_tier": "gold",
                "source_sequence": "2",
            }
        ]
        target = [
            {
                **source[0],
                "email": "target@example.test",
                "is_current": "true",
            }
        ]
        report = reconcile(source, target)

        self.assertFalse(report["passed"])
        self.assertEqual(report["difference_count"], 1)
        self.assertEqual(report["value_mismatches"][0]["field"], "email")


if __name__ == "__main__":
    unittest.main()

