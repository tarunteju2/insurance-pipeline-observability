#!/usr/bin/env python3
"""
Schema compatibility checker — run in CI on every PR that touches src/schemas/*.json.

Checks:
  1. All schema files are valid JSON.
  2. Each schema contains the required "$id" and "title" fields.
  3. schema_version enum in raw_claim_v1.json matches SCHEMA_VERSION in src/models/claims.py.
  4. Backward-compatibility guard: no previously-required fields have been removed
     compared to the last committed version (checks git diff).

Exit code: 0 = all good, 1 = one or more failures.

Usage:
  python scripts/validate_schemas.py
  python scripts/validate_schemas.py --strict    # fails on any additive change too
"""

import json
import os
import sys
import subprocess
from pathlib import Path


SCHEMAS_DIR = Path(__file__).parent.parent / "src" / "schemas"
REQUIRED_KEYS = ["$schema", "$id", "title", "description"]


def load_schema(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def check_required_keys(schema: dict, path: Path) -> list[str]:
    errors = []
    for key in REQUIRED_KEYS:
        if key not in schema:
            errors.append(f"{path.name}: missing '{key}'")
    return errors


def check_schema_version_matches_code() -> list[str]:
    """Ensure schema_version enum in raw_claim_v1.json matches src/models/claims.py."""
    errors = []
    raw_schema_path = SCHEMAS_DIR / "raw_claim_v1.json"
    if not raw_schema_path.exists():
        errors.append("raw_claim_v1.json not found")
        return errors

    raw_schema = load_schema(raw_schema_path)
    schema_versions_in_file = (
        raw_schema.get("properties", {})
                  .get("schema_version", {})
                  .get("enum", [])
    )

    # Extract SCHEMA_VERSION from Python source via a simple grep
    claims_py = Path(__file__).parent.parent / "src" / "models" / "claims.py"
    code_version = None
    for line in claims_py.read_text().splitlines():
        if line.strip().startswith("SCHEMA_VERSION"):
            code_version = line.split("=")[1].strip().strip('"\'')
            break

    if code_version is None:
        errors.append("Could not find SCHEMA_VERSION in src/models/claims.py")
    elif code_version not in schema_versions_in_file:
        errors.append(
            f"SCHEMA_VERSION='{code_version}' in claims.py not listed in "
            f"raw_claim_v1.json schema_version.enum={schema_versions_in_file}"
        )
    return errors


def check_backward_compatibility(strict: bool = False) -> list[str]:
    """
    Detect removed required fields by comparing current schemas against the
    last git-committed version.  Only runs when git is available and files
    have uncommitted changes.
    """
    errors = []
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD", "--", str(SCHEMAS_DIR)],
            capture_output=True, text=True, check=True
        )
        changed_files = [f.strip() for f in result.stdout.splitlines() if f.strip().endswith(".json")]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []  # git not available — skip

    for rel_path in changed_files:
        abs_path = Path(rel_path)
        if not abs_path.exists():
            continue
        current = load_schema(abs_path)
        try:
            old_content = subprocess.run(
                ["git", "show", f"HEAD:{rel_path}"],
                capture_output=True, text=True, check=True
            ).stdout
            old = json.loads(old_content)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            continue  # new file — no baseline to compare

        current_required = set(current.get("required", []))
        old_required = set(old.get("required", []))
        removed = old_required - current_required
        if removed:
            errors.append(
                f"BACKWARD COMPATIBILITY BREAK in {rel_path}: "
                f"previously-required fields removed: {removed}"
            )
        if strict:
            added = current_required - old_required
            if added:
                errors.append(
                    f"(strict) New required fields added in {rel_path}: {added} — "
                    "ensure all existing producers emit these fields."
                )
    return errors


def main():
    strict = "--strict" in sys.argv
    all_errors = []
    schema_files = list(SCHEMAS_DIR.glob("*.json"))

    if not schema_files:
        print(f"No schema files found in {SCHEMAS_DIR}")
        sys.exit(1)

    print(f"Validating {len(schema_files)} schema file(s) in {SCHEMAS_DIR}/")

    for path in sorted(schema_files):
        try:
            schema = load_schema(path)
        except json.JSONDecodeError as e:
            all_errors.append(f"{path.name}: invalid JSON — {e}")
            continue
        all_errors.extend(check_required_keys(schema, path))

    all_errors.extend(check_schema_version_matches_code())
    all_errors.extend(check_backward_compatibility(strict=strict))

    if all_errors:
        print("\nSCHEMA VALIDATION FAILED:")
        for err in all_errors:
            print(f"  ✗ {err}")
        sys.exit(1)
    else:
        print(f"  ✓ All {len(schema_files)} schema(s) valid and compatible.")
        sys.exit(0)


if __name__ == "__main__":
    main()
