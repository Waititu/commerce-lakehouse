# Runbook: Gold Analytics

## Prerequisites

1. Generate and validate source data (Phase 2).
2. Run `notebooks/03_bronze_ingestion.py` successfully.
3. Run `notebooks/04_silver_transformations.py` successfully.
4. Use the same Databricks catalog and `commerce_lakehouse` schema as the
   preceding phases.

## Execution

1. Open `notebooks/05_gold_analytics.py` in the Databricks workspace.
2. Attach it to the Databricks Free Edition serverless environment.
3. Run all cells from top to bottom.
4. Confirm the final `PASS` message and six model row-count lines.
5. Inspect the persisted tables in `<current_catalog>.commerce_lakehouse`.

The notebook uses the Databricks-provided `spark` session. Do not create a
local Spark session or use `spark.sparkContext` on serverless compute.

## Expected outputs

- `gold_daily_sales`
- `gold_customer_360`
- `gold_product_performance`
- `gold_payment_reconciliation`
- `gold_payment_status_daily`
- `gold_returns_summary`

Row counts depend on the actual input data and the aggregation grain.

## Failure handling

- **Missing Silver table:** run and verify Phase 4 before retrying.
- **Missing input column:** compare the persisted Silver schema against
  `REQUIRED_COLUMNS` and resolve schema drift before retrying.
- **Duplicate or null grain key:** do not accept the output as valid. Inspect
  source key quality and transformation logic.
- **Failure during writes:** some tables may already have been replaced because
  each table is committed independently. Rerun the complete notebook after
  resolving the cause; verify all six tables at the end.
- **Unexpected reconciliation differences:** investigate order status,
  payment status, refunds, currency, and timing. GMV and captured payments
  are distinct measures and are not expected to match in every case.

## Local checks

From `~/Desktop/commerce-lakehouse`:

```bash
.venv/bin/python -m py_compile notebooks/05_gold_analytics.py
.venv/bin/python -m pytest tests/data_quality tests/integration -v
.venv/bin/ruff check scripts notebooks tests
git diff --check
