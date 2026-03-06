.PHONY: help venv install run clean

PY=.venv/bin/python
PIP=.venv/bin/pip

help:
	@echo "Targets:"
	@echo "  make venv     - create .venv"
	@echo "  make install  - install requirements into .venv"
	@echo "  make run      - run the full pipeline (bronze->silver->dq->gold->summary)"
	@echo "  make clean    - remove generated artifacts (parquet/csv/reports summary)"

venv:
	python3 -m venv .venv
	@$(PY) -c "import sys; print('VENV python:', sys.executable)"

install: venv
	$(PIP) install -r requirements.txt

run: install
	$(PY) src/pipeline.py

clean:
	rm -f data/bronze/tch_repairs_raw.parquet
	rm -f data/silver/tch_repairs_cleaned.parquet
	rm -f data/gold/kpi_monthly.parquet data/gold/kpi_monthly.csv
	rm -f reports/artifacts_summary.txt reports/data_quality.json

.PHONY: deliver
deliver:
	@bash scripts/build_delivery.sh
