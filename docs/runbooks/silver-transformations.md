# Silver Transformation Runbook

## Prerequisites

1. Databricks Free Edition is available.
2. Notebook `03_bronze_ingestion` completed successfully.
3. All six `bronze_<dataset>` Delta tables exist in
   `commerce_lakehouse`.

## Execution

1. Open `notebooks/04_silver_transformations.py` in the repository.
2. Import or create a Databricks notebook named
   `04_silver_transformations` using the notebook source.
3. Attach the notebook to the supported serverless environment.
4. Run all cells from top to bottom.

The notebook discovers the current catalog and uses schema
`commerce_lakehouse`.

## Expected outputs

- Six `silver_<dataset>` managed Delta tables.
- Six `quarantine_<dataset>` managed Delta tables.
- A row-count reconciliation report for each source dataset.

For every dataset:

`Bronze row count = Silver row count + Quarantine row count`

## Troubleshooting

### Bronze table missing

Run Phase 3 successfully before running Phase 4. Confirm the selected
catalog and `commerce_lakehouse` schema are the same.

### Schema or column error

Compare the source columns with the Phase 3 Bronze tables. Do not change
Bronze source names to accommodate Silver naming conventions.

### Reconciliation failure

Stop and inspect the affected Silver and quarantine transformations.
Do not proceed to Gold until every dataset reconciles.

### Notebook write fails partway through

Writes occur independently. Rerun the complete notebook after resolving
the cause, then verify all twelve output tables and reconciliation counts.

## Cost control

This workflow uses Databricks Free Edition and managed tables in the
existing workspace. Do not provision paid cloud compute or external
infrastructure for this phase.
