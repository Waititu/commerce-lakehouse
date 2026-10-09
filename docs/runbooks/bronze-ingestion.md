# Runbook: Databricks Bronze Ingestion

## Prerequisites

- Databricks Free Edition workspace with serverless notebooks.
- Unity Catalog access sufficient to create a schema, volume, and managed tables.
- Six generated CSVs in local `data/raw/`.

## Import the notebook

1. In Databricks, open **Workspace** and choose **Import** (or the equivalent
   import action in the current UI).
2. Import `notebooks/03_bronze_ingestion.py` as a notebook.
3. Open it and select **Serverless** compute if it is not already selected.

## Create the landing volume and upload source files

1. Run notebook Cell 1. It discovers the current catalog, creates schema
   `commerce_lakehouse`, creates volume `raw_files`, and prints the full path.
2. Open **Catalog Explorer**, locate the printed catalog and schema, and open
   volume `raw_files`.
3. Use the volume's upload action to upload these six files from the local repo:
   `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`,
   `payments.csv`, `returns.csv`.
4. Confirm all six files are present in the volume. Do not upload sensitive data.

## Run ingestion and verify

1. Run notebook Cell 2.
2. A successful run prints six `PASS` lines and a final success message.
3. In a SQL cell, verify tables and row counts:

```sql
SHOW TABLES IN <catalog>.commerce_lakehouse;

SELECT 'customers' AS dataset, COUNT(*) AS row_count
FROM <catalog>.commerce_lakehouse.bronze_customers
UNION ALL SELECT 'products', COUNT(*)
FROM <catalog>.commerce_lakehouse.bronze_products
UNION ALL SELECT 'orders', COUNT(*)
FROM <catalog>.commerce_lakehouse.bronze_orders
UNION ALL SELECT 'order_items', COUNT(*)
FROM <catalog>.commerce_lakehouse.bronze_order_items
UNION ALL SELECT 'payments', COUNT(*)
FROM <catalog>.commerce_lakehouse.bronze_payments
UNION ALL SELECT 'returns', COUNT(*)
FROM <catalog>.commerce_lakehouse.bronze_returns;
```

Replace `<catalog>` with the catalog printed by Cell 1.

## Useful inspection queries

```sql
SELECT _source_file, _source_system, _ingestion_batch_id,
       MIN(_ingested_at) AS first_ingested_at,
       COUNT(*) AS rows_loaded
FROM <catalog>.commerce_lakehouse.bronze_orders
GROUP BY _source_file, _source_system, _ingestion_batch_id;

SELECT COUNT(*) AS rows, COUNT(DISTINCT _record_hash) AS distinct_hashes
FROM <catalog>.commerce_lakehouse.bronze_orders;
```

## Troubleshooting

- **Catalog error:** ensure the notebook's current catalog is a Unity Catalog
  catalog and your user can create objects in it.
- **Permission denied:** choose a catalog/schema where you have CREATE SCHEMA,
  CREATE VOLUME, and table-creation permissions, then rerun Cell 1.
- **Missing file:** verify the file is uploaded into the exact printed volume
  path; local paths such as `~/Desktop/commerce-lakehouse/data/raw` are not
  visible from Databricks serverless compute.
- **Schema drift:** regenerate the six files with the committed local generator,
  re-upload all six, then rerun ingestion.
- **Cast or relationship failure:** inspect the source CSV and its contract
  in `docs/architecture/source-data-contracts.md`; do not silently discard rows.
- **Partial table replacement:** after resolving the error, rerun the complete
  ingestion cell so all six tables reflect the same input snapshot.
- **Quota/compute unavailable:** stop and resume when the Free Edition quota
  resets. Do not switch to paid compute for this lab.

## Reset

To replace Bronze tables, rerun Cell 2. It overwrites the six managed tables
from the current volume files. It does not delete the source CSVs or schema.
