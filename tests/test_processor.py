from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from cdc_reconciliation.generator import CDCConfig, generate_cdc_dataset
from cdc_reconciliation.io import read_csv
from cdc_reconciliation.processor import CDCProcessor


class CDCProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        root = Path(self.temporary_directory.name)
        self.input_dir = root / "input"
        self.output_dir = root / "warehouse"
        self.generator_manifest = generate_cdc_dataset(
            self.input_dir,
            CDCConfig(seed=2026, entities=24),
        )
        self.processor = CDCProcessor(self.output_dir)

    def run_pipeline(self) -> dict[str, object]:
        return self.processor.run(
            self.input_dir / "events.jsonl",
            self.input_dir / "source_snapshot.csv",
        )

    def test_ordering_dedup_scd2_and_reconciliation(self) -> None:
        manifest = self.run_pipeline()
        history = read_csv(self.output_dir / "silver" / "customer_history.csv")
        entity_two = [row for row in history if row["entity_id"] == "C00002"]

        self.assertEqual(len(entity_two), 2)
        self.assertEqual(entity_two[0]["is_current"], "false")
        self.assertEqual(entity_two[0]["valid_to"], entity_two[1]["valid_from"])
        self.assertEqual(entity_two[1]["source_sequence"], "2")
        self.assertTrue(entity_two[1]["loyalty_tier"])
        self.assertGreater(manifest["duplicate_deliveries_ignored"], 0)
        self.assertTrue(manifest["reconciliation_passed"])

        report = json.loads(
            (
                self.output_dir / "_meta" / "reconciliation_report.json"
            ).read_text()
        )
        self.assertEqual(report["difference_count"], 0)
        self.assertEqual(
            report["target_active_count"],
            self.generator_manifest["expected_active_entities"],
        )

    def test_unsupported_schema_is_quarantined(self) -> None:
        manifest = self.run_pipeline()
        quarantine = read_csv(self.output_dir / "quarantine" / "events.csv")

        self.assertEqual(
            manifest["newly_quarantined_events"],
            self.generator_manifest["intentional_unsupported_schema_events"],
        )
        self.assertTrue(
            all(row["reason"].startswith("unsupported_schema_version") for row in quarantine)
        )

    def test_rerun_has_exactly_once_style_target_effect(self) -> None:
        first = self.run_pipeline()
        history_path = self.output_dir / "silver" / "customer_history.csv"
        ledger_path = self.output_dir / "_meta" / "event_ledger.csv"
        first_history = history_path.read_bytes()
        first_ledger = ledger_path.read_bytes()

        second = self.run_pipeline()

        self.assertEqual(first_history, history_path.read_bytes())
        self.assertEqual(first_ledger, ledger_path.read_bytes())
        self.assertEqual(second["newly_applied_events"], 0)
        self.assertEqual(second["newly_quarantined_events"], 0)
        self.assertEqual(
            first["output_checksums"]["silver/customer_history.csv"],
            second["output_checksums"]["silver/customer_history.csv"],
        )

    def test_delete_closes_current_record(self) -> None:
        self.run_pipeline()
        history = read_csv(self.output_dir / "silver" / "customer_history.csv")
        entity_eleven = [row for row in history if row["entity_id"] == "C00011"]

        self.assertEqual(len(entity_eleven), 1)
        self.assertEqual(entity_eleven[0]["is_current"], "false")
        self.assertTrue(entity_eleven[0]["valid_to"])


if __name__ == "__main__":
    unittest.main()

