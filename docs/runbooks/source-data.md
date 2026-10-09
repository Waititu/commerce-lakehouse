# Source Data Generation and Validation

## Purpose

CommerceLake uses deterministic synthetic e-commerce data to exercise
ingestion, data quality, transformations, and analytical reconciliation
without relying on customer data or paid datasets.

## Requirements

- Python 3.12
- Project virtual environment at `.venv`
- Repository root as the working directory

## Generate the datasets

```bash
.venv/bin/python scripts/generate_synthetic_data.py
