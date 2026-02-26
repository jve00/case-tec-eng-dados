PYTHON ?= python3
VENV ?= .venv

ifeq ($(OS),Windows_NT)
VENV_BIN := $(VENV)/Scripts
else
VENV_BIN := $(VENV)/bin
endif

PY := $(VENV_BIN)/python
PIP := $(VENV_BIN)/pip
SPARK_SUBMIT := $(VENV_BIN)/spark-submit

.DEFAULT_GOAL := help

help:
	@echo "Targets disponíveis:"
	@echo "  make setup          - Cria venv e instala dependências"
	@echo "  make check-java     - Valida pré-requisito de Java"
	@echo "  make generate-data  - Gera dataset sintético em data/payments"
	@echo "  make run            - Executa job via spark-submit"
	@echo "  make test           - Executa validação + testes"
	@echo "  make lint           - Executa ruff + black --check"

setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

check-java:
ifeq ($(OS),Windows_NT)
	@java -version > NUL 2>&1 || (echo "Java 11+ é obrigatório para Spark. Instale Java e configure JAVA_HOME."; exit 1)
else
	@java -version >/dev/null 2>&1 || (echo "Java 11+ é obrigatório para Spark. Instale Java e configure JAVA_HOME."; exit 1)
endif

generate-data: check-java
	$(PY) src/generate_data.py --out-dir data/payments --seed 42 --months 12 --schools 30 --rows 12000 --start-month 2024-01

check-solution:
	$(PY) scripts/check_solution_constraints.py

run: check-java check-solution
ifeq ($(OS),Windows_NT)
	$(SPARK_SUBMIT) --master local[*] --conf spark.sql.shuffle.partitions=8 src/job.py --input-path data/payments --output-path data/output
else
	./scripts/run_spark_submit.sh data/payments data/output
endif

test: check-java check-solution
	$(PY) -m pytest tests

lint:
	$(PY) -m ruff check src tests
	$(PY) -m black --check src tests
