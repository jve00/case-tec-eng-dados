#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_PATH="${1:-data/payments}"
OUTPUT_PATH="${2:-data/output}"

if [[ "${INPUT_PATH}" != /* ]]; then
  INPUT_PATH="${ROOT_DIR}/${INPUT_PATH}"
fi

if [[ "${OUTPUT_PATH}" != /* ]]; then
  OUTPUT_PATH="${ROOT_DIR}/${OUTPUT_PATH}"
fi

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-${ROOT_DIR}/.venv/bin/spark-submit}"
if [[ ! -x "${SPARK_SUBMIT_BIN}" ]]; then
  SPARK_SUBMIT_BIN="spark-submit"
fi

# Ensure pip-installed PySpark resolves SPARK_HOME using the project virtualenv Python.
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"
if [[ -x "${VENV_PYTHON}" ]]; then
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${VENV_PYTHON}}"
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${VENV_PYTHON}}"
fi

"${SPARK_SUBMIT_BIN}" \
  --master local[*] \
  --conf spark.sql.shuffle.partitions=8 \
  "${ROOT_DIR}/src/job.py" \
  --input-path "${INPUT_PATH}" \
  --output-path "${OUTPUT_PATH}"
