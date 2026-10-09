# CommerceLake Gold Layer

## Purpose

Gold contains business-facing analytical models built from validated Silver
Delta tables. Each model has a declared grain and is stored as a managed
Delta table in `<current_catalog>.commerce_lakehouse`.

Run `notebooks/05_gold_analytics.py` after Phase 4 has completed.

## Models and grain

| Model | Grain | Purpose |
|---|---|---|
| `gold_daily_sales` | Order date, currency, order status | Orders, units, item count, GMV and average order GMV |
| `gold_customer_360` | Customer | Customer attributes, order activity, cumulative GMV and first/last order |
| `gold_product_performance` | Product | Units sold/returned, GMV, refunds and GMV after refunds |
| `gold_payment_reconciliation` | Order | Item GMV, payment amounts, captured amounts and refunds |
| `gold_payment_status_daily` | Payment date, status, currency, method | Daily payment counts and amounts |
| `gold_returns_summary` | Return date, reason, product category, currency | Return event counts, returned quantities and refunds |

All Gold tables include `_gold_run_id` and `_gold_built_at`.

## Metric definitions

- **Gross merchandise value (GMV):** sum of `quantity * unit_price` from
  Silver order items, before subtracting refunds.
- **Average order GMV:** arithmetic mean of order-level GMV within the
  relevant aggregation group.
- **Lifetime GMV:** cumulative item GMV for a customer. It is not profit or a
  prediction of future value.
- **Captured payment amount:** sum of payment amounts whose normalized status
  is exactly `captured`.
- **Captured less refunds:** captured payment amount minus recorded refund
  amounts. It is not a full accounting ledger.
- **Captured-to-GMV difference:** captured payment amount minus item GMV.
  Investigate nonzero differences rather than assuming they are errors.
- **GMV after refunds:** item GMV minus recorded refunds. It excludes costs,
  taxes, shipping, and other accounting adjustments.

Amounts remain grouped by currency where currencies are relevant. Do not sum
different currencies into a single financial total without conversion and a
documented exchange-rate policy.

## Join and aggregation safety

Order items are aggregated to order grain before joining to orders.
Payments are independently aggregated to order grain. Returns are aggregated
to order-item grain before joining to product sales, and to order grain before
payment reconciliation. These boundaries prevent one-to-many join fan-out
from inflating sales, payment, or refund measures.

## Persistence and consistency

Each model is written as a managed Delta table using overwrite semantics.
Each table write is its own transaction; all six writes are not atomic as a
group. If execution fails midway, rerun the notebook to rebuild the snapshots.
The notebook verifies persisted row counts, key non-nullability, uniqueness
at the declared grain, and build metadata.

## Known limitations and next improvements

- Captured payment status is defined as the exact normalized value `captured`.
  Confirm this matches the upstream status vocabulary.
- Refund amounts are assumed to be denominated in the associated order's
  currency; returns have no independent currency field.
- GMV does not establish accounting revenue recognition.
- Current models are full snapshots, not incremental upserts.
- Cross-table atomic publication, freshness SLAs, and formal metric contracts
  belong in subsequent orchestration, testing, and governance phases.
