"""Synthetic CDC processing and reconciliation reference implementation."""

from cdc_reconciliation.generator import CDCConfig, generate_cdc_dataset
from cdc_reconciliation.processor import CDCProcessor

__all__ = ["CDCConfig", "CDCProcessor", "generate_cdc_dataset"]
__version__ = "0.1.0"

