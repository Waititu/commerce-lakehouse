#!/usr/bin/env python3
"""Validate CommerceLake raw source datasets against their canonical contracts."""

import csv
import sys
from pathlib import Path

from commerce_lakehouse.validation import load_csv_datasets, validate_datasets

ROOT = Path(__file__).resolve().parents[1]
DATA_DIRECTORY = ROOT / "data" / "raw"


def main() -> int:
    print("=== CommerceLake Source Data Validation ===")

    try:
        headers, datasets = load_csv_datasets(DATA_DIRECTORY)
    except (OSError, csv.Error, UnicodeError) as exc:
        print(f"FAIL: unable to load source data: {exc}")
        return 1

    for name in (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "returns",
    ):
        rows = datasets.get(name, [])
        print(f"{name:12} {len(rows):>7,} rows")

    try:
        errors = validate_datasets(headers, datasets)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        print(f"\nFAIL: unable to validate malformed source data: {exc}")
        return 1

    if errors:
        print(f"\nFAIL: {len(errors)} validation error(s)")
        for error in errors[:50]:
            print(f"  - {error}")
        if len(errors) > 50:
            print(f"  ... and {len(errors) - 50} more")
        return 1

    print("\nPASS: all source data validation checks succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
