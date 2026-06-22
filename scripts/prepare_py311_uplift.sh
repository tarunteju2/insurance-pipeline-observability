#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv311"
AIRFLOW_VERSION="${1:-2.11.1}"
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
TMP_REQUIREMENTS="$(mktemp)"

cleanup() {
  rm -f "$TMP_REQUIREMENTS"
}
trap cleanup EXIT

echo "[1/6] verify python3.11"
command -v python3.11 >/dev/null 2>&1 || {
  echo "python3.11 not found. Install Python 3.11 first."
  exit 1
}

echo "[2/6] create venv: $VENV_DIR"
python3.11 -m venv "$VENV_DIR"

echo "[3/6] upgrade pip tooling"
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel

echo "[4/6] generate py311 uplift requirements"
awk -v airflow_version="$AIRFLOW_VERSION" -v constraints_url="$CONSTRAINTS_URL" '
  BEGIN {
    print "# Airflow dependency constraints (Python 3.11)"
    print "--constraint " constraints_url
  }
  /^--constraint / { next }
  /^opentelemetry-instrumentation-/ { next }
  /^opentelemetry-exporter-/ { next }
  /^apache-airflow==/ { print "apache-airflow==" airflow_version; next }
  /^apache-airflow-providers-postgres==/ { print "apache-airflow-providers-postgres"; next }
  /^apache-airflow-providers-amazon==/ { print "apache-airflow-providers-amazon"; next }
  /^[[:space:]]*#/ { print; next }
  /^[[:space:]]*$/ { print; next }
  {
    line = $0
    sub(/[[:space:]]+#.*/, "", line)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
    if (line ~ /^[A-Za-z0-9_.\[\]-]+==/) {
      sub(/==.*/, "", line)
      print line
      next
    }
  }
  { print }
' "$ROOT_DIR/requirements.txt" > "$TMP_REQUIREMENTS"

echo "[5/6] install uplift dependencies"
"$VENV_DIR/bin/python" -m pip install -r "$TMP_REQUIREMENTS"

echo "[6/6] validate env"
"$VENV_DIR/bin/python" -m pip check

echo "done"
echo "venv: $VENV_DIR"
echo "constraints: $CONSTRAINTS_URL"
echo "run tests: $VENV_DIR/bin/python -m pytest -q"
