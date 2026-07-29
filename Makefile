.PHONY: help generate run demo compile test check lint clean

PYTHON ?= python3
SEED ?= 20260728

help:
	@echo "Synthetic CDC Lakehouse Reconciliation"
	@echo "  make demo      Generate and process a deterministic CDC batch"
	@echo "  make check     Compile and run dependency-free tests"
	@echo "  make test      Run dependency-free tests"
	@echo "  make lint      Run ruff when installed"
	@echo "  make clean     Remove local generated artifacts"

generate:
	PYTHONPATH=src $(PYTHON) -m cdc_reconciliation.cli generate \
		--output data/input --seed $(SEED) --entities 24

run:
	PYTHONPATH=src $(PYTHON) -m cdc_reconciliation.cli run \
		--events data/input/events.jsonl \
		--source-snapshot data/input/source_snapshot.csv \
		--output warehouse

demo:
	PYTHONPATH=src $(PYTHON) -m cdc_reconciliation.cli demo \
		--workspace .demo --seed $(SEED) --entities 24

compile:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
		$(PYTHON) -m compileall -q src tests

test:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
		$(PYTHON) -m unittest discover -s tests -v

check: compile test

lint:
	$(PYTHON) -m ruff check src tests

clean:
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('data/input', 'warehouse', '.demo', 'build', 'dist')]"
