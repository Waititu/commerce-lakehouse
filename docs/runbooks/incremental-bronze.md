# Runbook: Incremental Bronze Ingestion

## Prerequisites

- Databricks Free Edition with a Unity Catalog catalog selected.
- Access to `commerce_lakehouse.raw_files` and its six CSVs: customers, products,
  orders, order_items, payments, and returns.
- The existing source headers and declared types match `06_incremental_bronze.py`.
- The workspace supports `delta.tables.DeltaTable` and Delta `MERGE` in serverless.

## Execute

1. Import `notebooks/06_incremental_bronze.py` into Databricks.
2. Select serverless notebook compute and run all cells.
3. Leave `batch_id_override` blank for a new batch, or set a stable value such as
   `phase6-replay-20261009` to make a replay's audit identity stable.
4. Confirm the notebook prints `PASS` for all six datasets and the final batch check.

## Inspect the audit

Replace `<catalog>` with the catalog selected in the notebook:

```sql
SELECT batch_id, dataset, status, source_rows, inserted_rows, updated_rows,
       unchanged_rows, target_rows_after, error_message, started_at, completed_at
FROM <catalog>.commerce_lakehouse.incremental_batch_audit
ORDER BY started_at DESC, dataset;
```

For one batch, add `WHERE batch_id = '<batch-id>'`. All six rows should have
`status = 'SUCCESS'`.

## Replay/idempotency verification

1. Run once with `batch_id_override = phase6-idempotency-check`.
2. Run again with the same value and unchanged CSV files.
3. Confirm all six audit rows are successful; on the second run, expect zero inserts
   and zero updates, with all existing input rows counted as unchanged.
4. Confirm target row counts are unchanged.

```sql
SELECT 'customers' AS dataset, COUNT(*) AS rows FROM <catalog>.commerce_lakehouse.bronze_customers
UNION ALL SELECT 'products', COUNT(*) FROM <catalog>.commerce_lakehouse.bronze_products
UNION ALL SELECT 'orders', COUNT(*) FROM <catalog>.commerce_lakehouse.bronze_orders
UNION ALL SELECT 'order_items', COUNT(*) FROM <catalog>.commerce_lakehouse.bronze_order_items
UNION ALL SELECT 'payments', COUNT(*) FROM <catalog>.commerce_lakehouse.bronze_payments
UNION ALL SELECT 'returns', COUNT(*) FROM <catalog>.commerce_lakehouse.bronze_returns;
```

## Recovery

- **Validation error:** correct the input and rerun. Source validation happens before
  the first Bronze mutation.
- **A dataset merge fails:** inspect the audit table. Rerun with the same batch ID;
  previously committed datasets are safe to merge again.
- **A run is interrupted:** a dataset may remain `STARTED`; replay the batch and verify
  all six end as `SUCCESS`.
- **Target schema drift:** compare the table with the expected source columns plus
  lineage columns. Do not automatically drop/recreate a production table.
- **Concurrent runs:** do not run two writers against the same catalog/schema.

This notebook updates Bronze only. Run the established Silver and Gold notebooks
separately if downstream models need refreshing.
