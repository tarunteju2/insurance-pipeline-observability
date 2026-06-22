# Phase 3: Python 3.11 + Airflow Security Uplift

## Goal
- Move runtime from Python 3.9 lock to Python 3.11.
- Lift Airflow baseline above 2.8.1 to reduce vulnerability surface.
- Keep pipeline behavior stable (tests green, DAGs import, local stack starts).

## Current State
- Stable baseline runs on constraints-3.9 with Airflow 2.8.1.
- Full test suite currently passes on baseline.
- Vulnerability count remains high due old constrained stack.

## Execution Steps

### 1) Create isolated uplift env
```bash
./scripts/prepare_py311_uplift.sh 2.11.1
```

What script does:
- Creates `.venv311` with Python 3.11.
- Rewrites constraints to `constraints-3.11.txt` for selected Airflow version.
- Keeps direct dependencies, but unpins Airflow providers to avoid hard conflicts.
- Installs and runs `pip check`.

### 2) Validate application behavior in uplift env
```bash
.venv311/bin/python -m pytest -q
.venv311/bin/python -m pytest tests/test_pipeline.py -q
```

### 3) Validate DAG import + Airflow CLI
```bash
.venv311/bin/airflow version
.venv311/bin/airflow dags list
```

### 4) Re-audit vulnerabilities
```bash
.venv311/bin/python -m pip_audit -r requirements.txt
```

### 5) Freeze uplift lock once stable
```bash
.venv311/bin/python -m pip freeze > requirements_py311_lock.txt
```

## Acceptance Criteria
- `pip check` returns zero errors in `.venv311`.
- `pytest -q` passes with no regressions.
- `airflow dags list` succeeds.
- Vulnerability count materially lower than Python 3.9 baseline.

## Rollback
- Keep baseline `requirements.txt` unchanged until uplift criteria pass.
- Remove uplift env only:
```bash
rm -rf .venv311
```
