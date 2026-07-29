from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cdc_reconciliation.generator import CDCConfig, generate_cdc_dataset
from cdc_reconciliation.io import sha256_file


class CDCGeneratorTests(unittest.TestCase):
    def test_generator_is_byte_deterministic(self) -> None:
        config = CDCConfig(seed=41, entities=12)
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_path = Path(first)
            second_path = Path(second)
            first_manifest = generate_cdc_dataset(first_path, config)
            second_manifest = generate_cdc_dataset(second_path, config)

            self.assertEqual(first_manifest, second_manifest)
            self.assertEqual(
                sha256_file(first_path / "events.jsonl"),
                sha256_file(second_path / "events.jsonl"),
            )
            self.assertEqual(
                sha256_file(first_path / "source_snapshot.csv"),
                sha256_file(second_path / "source_snapshot.csv"),
            )

    def test_generator_documents_delivery_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = generate_cdc_dataset(
                Path(directory),
                CDCConfig(seed=7, entities=10, duplicate_every=3),
            )

        self.assertTrue(manifest["synthetic"])
        self.assertGreater(manifest["intentional_duplicate_deliveries"], 0)
        self.assertEqual(manifest["intentional_unsupported_schema_events"], 2)
        self.assertGreater(
            manifest["delivered_events"],
            manifest["logical_valid_events"],
        )


if __name__ == "__main__":
    unittest.main()

