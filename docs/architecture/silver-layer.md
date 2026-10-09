# CommerceLake Silver Layer

## Purpose

Silver standardizes Bronze records, applies business and relational
quality rules, quarantines rejected records, and publishes accepted
records as managed Delta tables.

Bronze remains unchanged and is the replayable input layer.

## Input and output

For each dataset, Silver reads `bronze_<dataset>` and writes:

- `silver_<dataset>`: accepted and standardized records.
- `quarantine_<dataset>`: rejected records with rejection reasons.

Datasets: customers, products, orders, order_items, payments, returns.

## Standardized fields

- `orders.order_status` becomes `orders.status`.
- `payments.payment_status` becomes `payments.status`.
- `returns.quantity_returned` becomes `returns.returned_quantity`.
- `returns.return_reason` becomes `returns.reason`.

Text values are trimmed. Country and currency codes are uppercased;
categorical values are lowercased where specified.

## Quality rules

- Required keys and business fields must be populated.
- Product prices must be positive.
- Order-item quantities must be positive; item prices cannot be negative.
- Payment amounts and refund amounts cannot be negative.
- Timestamps required by each dataset must be present.
- Foreign keys must resolve to accepted parent records.
- Returned quantity cannot exceed the purchased quantity for an item.
- Every quarantined record must have a rejection reason.

## Lineage

Silver records retain Bronze lineage fields, including `_record_hash`,
`_ingestion_batch_id`, `_source_file`, and `_ingested_at`, and add
`_silver_processed_at` and `_silver_run_id`.

Quarantine records include `_silver_reject_reason`, `_quarantined_at`,
`_quarantine_run_id`, and `_quarantine_dataset`.

## Processing semantics

This phase uses full-snapshot overwrite semantics, not incremental
merge semantics. Rerun the notebook to rebuild the snapshots from Bronze.

Each table write is a separate Delta transaction. The entire collection
of Silver and quarantine tables is not committed atomically as a unit.
If a write fails, rerun the notebook and verify all table counts.

## Scope

The notebook validates the documented row-level rules and selected
foreign-key relationships. Additional business rules, including
controlled status vocabularies, payment reconciliation, currency
consistency, and aggregate return limits across multiple return events,
should be enforced in dedicated quality tests or subsequent phases.
