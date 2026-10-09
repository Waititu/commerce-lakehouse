# CommerceLake Bronze Layer

## Purpose

Bronze is the first persistent lakehouse layer. It stores typed records from
the six synthetic source CSVs as Unity Catalog managed Delta tables. The
uploaded CSV files remain the reprocessing/landing copy.

## Data flow

```text
Local data/raw/*.csv
        |
        | manual upload through Databricks UI
        v
Unity Catalog volume: commerce_lakehouse.raw_files
        |
        | explicit casts + schema, volume, PK, FK, and count checks
        v
Managed Delta tables: commerce_lakehouse.bronze_*
        |
        v
Silver transformations (Phase 4)
```

## Table inventory

| Source | Bronze table | Expected rows | Primary key |
|---|---|---:|---|
| customers.csv | bronze_customers | 1,000 | customer_id |
| products.csv | bronze_products | 200 | product_id |
| orders.csv | bronze_orders | 5,000 | order_id |
| order_items.csv | bronze_order_items | 12,489 | order_item_id |
| payments.csv | bronze_payments | 5,671 | payment_id |
| returns.csv | bronze_returns | 395 | return_id |

All tables include `_ingested_at`, `_source_file`, `_source_system`,
`_ingestion_batch_id`, and `_record_hash`.

## Ingestion semantics

- CSV parsing does not infer types. Column types are explicitly declared.
- IDs remain strings; money uses `DECIMAL(12,2)`; timestamps use `TIMESTAMP`.
- Schema drift, cast failures, unexpected row counts, null/duplicate primary
  keys, and unresolved foreign keys fail the notebook before writes begin.
- Bronze writes use full-snapshot overwrite. Reruns do not append duplicates.
- A successful rerun replaces current Bronze snapshots and creates a new batch
  identifier. It does **not** retain historical ingestion batches.
- The six table writes are not a single cross-table transaction. A write failure
  partway through persistence can leave a mixed set of snapshots; rerun after
  fixing the cause to restore a consistent complete snapshot.

## Operational boundaries

This is a reproducible portfolio implementation using Databricks Free Edition.
It is not a claim of enterprise production SLAs. Batch history, incremental
processing, quarantine, and orchestration are later roadmap concerns.
