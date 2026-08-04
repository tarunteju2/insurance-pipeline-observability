#!/usr/bin/env bash
set -eo pipefail

echo "====================================================="
echo " Phase 3 Python 3.11 + Airflow 2.11.1 Uplift Verification"
echo "====================================================="

VENV_DIR=".venv311"

if [ ! -d "$VENV_DIR" ]; then
    echo "❌ Error: Virtual environment $VENV_DIR not found. Run scripts/prepare_py311_uplift.sh first."
    exit 1
fi

echo "1. Checking Python version..."
"$VENV_DIR/bin/python" --version

echo "2. Running dependency check (pip check)..."
"$VENV_DIR/bin/pip" check

echo "3. Running Pytest suite..."
"$VENV_DIR/bin/python" -m pytest -q

echo "4. Testing Airflow DAG loading..."
"$VENV_DIR/bin/airflow" dags list > /dev/null 2>&1 && echo "✅ Airflow DAGs successfully parsed."

echo "====================================================="
echo " 🎉 Phase 3 Verification Completed Successfully!"
echo "====================================================="
