# CommerceLake Source Data Contracts

## 1. Purpose

Define the source entities, keys, relationships, data types,
and business rules for the CommerceLake e-commerce platform.

All initial records are synthetic. Timestamps use UTC.
Monetary values use fixed-precision decimal types.

## 2. Entities

### customers

Grain: one row per customer.

- customer_id: STRING, primary key, required
- signup_timestamp: TIMESTAMP, required
- country_code: STRING, required, ISO 3166-1 alpha-2
- customer_segment: STRING, required

Initial segment values:
- standard
- premium
- business

### products

Grain: one row per product.

- product_id: STRING, primary key, required
- product_name: STRING, required
- category: STRING, required
- unit_price: DECIMAL(12,2), required, non-negative
- currency: STRING, required, ISO 4217
- is_active: BOOLEAN, required

### orders

Grain: one row per order.

- order_id: STRING, primary key, required
- customer_id: STRING, required, foreign key to customers
- order_timestamp: TIMESTAMP, required
- order_status: STRING, required
- currency: STRING, required, ISO 4217

Initial order status values:
- pending
- confirmed
- shipped
- delivered
- cancelled

Order status represents the order lifecycle, not payment status.

### order_items

Grain: one row per product line in an order.

- order_item_id: STRING, primary key, required
- order_id: STRING, required, foreign key to orders
- product_id: STRING, required, foreign key to products
- quantity: INT, required, greater than zero
- unit_price: DECIMAL(12,2), required, non-negative

The item price is the price recorded at purchase time.
It is not automatically replaced by the current catalog price.

Line gross amount = quantity * unit_price.

### payments

Grain: one row per payment attempt.

- payment_id: STRING, primary key, required
- order_id: STRING, required, foreign key to orders
- payment_timestamp: TIMESTAMP, required
- payment_status: STRING, required
- amount: DECIMAL(12,2), required, non-negative
- currency: STRING, required, ISO 4217
- payment_method: STRING, required

Initial payment status values:
- initiated
- authorized
- captured
- failed
- refunded

Multiple payment attempts per order are permitted.
A failed attempt is not recognized revenue.
Refunds must be modeled carefully to avoid double counting.

### returns

Grain: one row per return event for an order item.

- return_id: STRING, primary key, required
- order_item_id: STRING, required, foreign key to order_items
- return_timestamp: TIMESTAMP, required
- quantity_returned: INT, required, greater than zero
- refund_amount: DECIMAL(12,2), required, non-negative
- return_reason: STRING, required

Cumulative returned quantity must not exceed
the purchased quantity for the associated order item.

## 3. Referential integrity

- Every order references an existing customer.
- Every order item references an existing order and product.
- Every payment references an existing order.
- Every return references an existing order item.

## 4. Data quality rules

- Primary keys must be non-null and unique.
- Required fields must not be null.
- Quantities must be positive integers.
- Monetary values must use decimal arithmetic.
- Timestamps must be valid and interpreted as UTC.
- Status fields must use documented allowed values.
- Foreign keys must resolve.
- Currency must be consistent across related monetary calculations.
- Invalid source records must be measurable and traceable.

## 5. Revenue definitions

- Item gross value = quantity * purchase-time unit price.
- Captured payment is measured from successful captured payment events.
- Refund value is measured separately from captured payments.
- Net cash collected = captured payments - refunds, subject to
  consistent event and currency handling.
- Order value, captured payments, and refunds are distinct measures.

The Gold revenue model will define its reporting grain and reconciliation
rules before aggregating these measures.

## 6. Reproducibility

Synthetic data generation must use a fixed seed.
Expected row counts, key relationships, and deliberate quality defects
must be documented and tested.
