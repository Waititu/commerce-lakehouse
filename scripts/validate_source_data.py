#!/usr/bin/env python3
"""Validate CommerceLake synthetic source datasets."""

import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data" / "raw"

ALLOWED_SEGMENTS = {"standard", "premium", "business"}
ALLOWED_ORDER_STATUSES = {
    "pending", "confirmed", "shipped", "delivered", "cancelled"
}
ALLOWED_PAYMENT_STATUSES = {
    "initiated", "authorized", "captured", "failed", "refunded"
}
ALLOWED_COUNTRIES = {"KE", "US", "GB", "CA", "DE"}
ALLOWED_CURRENCIES = {"USD", "KES", "GBP", "EUR", "CAD"}


def load(name):
    path = DATA / f"{name}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parse_timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("timestamp has no timezone")
    return parsed


def parse_decimal(value):
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError("non-finite monetary value")
    return number


def check_unique(rows, field, dataset, errors):
    values = [row[field] for row in rows]
    if any(not value for value in values):
        errors.append(f"{dataset}.{field}: null or empty primary key")
    if len(values) != len(set(values)):
        errors.append(f"{dataset}.{field}: duplicate primary keys")


def check_required(rows, dataset, fields, errors):
    for line_number, row in enumerate(rows, start=2):
        for field in fields:
            if not row.get(field, "").strip():
                errors.append(
                    f"{dataset} CSV line {line_number}: {field} is empty"
                )


def main():
    errors = []

    try:
        customers = load("customers")
        products = load("products")
        orders = load("orders")
        items = load("order_items")
        payments = load("payments")
        returns = load("returns")
    except (OSError, csv.Error) as exc:
        print(f"FAIL: unable to load source data: {exc}")
        return 1

    datasets = {
        "customers": (customers, ["customer_id"]),
        "products": (products, ["product_id"]),
        "orders": (orders, ["order_id"]),
        "order_items": (items, ["order_item_id"]),
        "payments": (payments, ["payment_id"]),
        "returns": (returns, ["return_id"]),
    }

    required = {
        "customers": [
            "customer_id", "signup_timestamp", "country_code",
            "customer_segment",
        ],
        "products": [
            "product_id", "product_name", "category", "unit_price",
            "currency", "is_active",
        ],
        "orders": [
            "order_id", "customer_id", "order_timestamp",
            "order_status", "currency",
        ],
        "order_items": [
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price",
        ],
        "payments": [
            "payment_id", "order_id", "payment_timestamp",
            "payment_status", "amount", "currency", "payment_method",
        ],
        "returns": [
            "return_id", "order_item_id", "return_timestamp",
            "quantity_returned", "refund_amount", "return_reason",
        ],
    }

    for name, (rows, keys) in datasets.items():
        for key in keys:
            check_unique(rows, key, name, errors)
        check_required(rows, name, required[name], errors)

    customer_by_id = {row["customer_id"]: row for row in customers}
    product_by_id = {row["product_id"]: row for row in products}
    order_by_id = {row["order_id"]: row for row in orders}
    item_by_id = {row["order_item_id"]: row for row in items}

    def fk(rows, field, parent, dataset):
        for row in rows:
            if row[field] not in parent:
                errors.append(
                    f"{dataset}: {field} references unknown ID {row[field]}"
                )

    fk(orders, "customer_id", customer_by_id, "orders")
    fk(items, "order_id", order_by_id, "order_items")
    fk(items, "product_id", product_by_id, "order_items")
    fk(payments, "order_id", order_by_id, "payments")
    fk(returns, "order_item_id", item_by_id, "returns")

    for row in customers:
        if row["country_code"] not in ALLOWED_COUNTRIES:
            errors.append(f"customers: invalid country {row['country_code']}")
        if row["customer_segment"] not in ALLOWED_SEGMENTS:
            errors.append(
                f"customers: invalid segment {row['customer_segment']}"
            )
        try:
            parse_timestamp(row["signup_timestamp"])
        except (ValueError, TypeError):
            errors.append("customers: invalid signup timestamp")

    for row in products:
        try:
            if parse_decimal(row["unit_price"]) < 0:
                errors.append("products: negative unit price")
        except (InvalidOperation, ValueError):
            errors.append("products: invalid unit price")
        if row["currency"] not in ALLOWED_CURRENCIES:
            errors.append(f"products: invalid currency {row['currency']}")
        if row["is_active"] not in {"true", "false"}:
            errors.append("products: is_active must be true or false")

    for row in orders:
        if row["order_status"] not in ALLOWED_ORDER_STATUSES:
            errors.append(f"orders: invalid status {row['order_status']}")
        if row["currency"] not in ALLOWED_CURRENCIES:
            errors.append(f"orders: invalid currency {row['currency']}")
        try:
            order_time = parse_timestamp(row["order_timestamp"])
            customer = customer_by_id.get(row["customer_id"])
            if customer and order_time < parse_timestamp(
                customer["signup_timestamp"]
            ):
                errors.append(
                    f"orders: {row['order_id']} predates customer signup"
                )
        except (ValueError, TypeError):
            errors.append(f"orders: invalid timestamp for {row['order_id']}")

    for row in items:
        try:
            if int(row["quantity"]) <= 0:
                errors.append("order_items: quantity must be positive")
        except ValueError:
            errors.append("order_items: quantity must be an integer")
        try:
            if parse_decimal(row["unit_price"]) < 0:
                errors.append("order_items: negative unit price")
        except (InvalidOperation, ValueError):
            errors.append("order_items: invalid unit price")

    for row in payments:
        if row["payment_status"] not in ALLOWED_PAYMENT_STATUSES:
            errors.append(f"payments: invalid status {row['payment_status']}")
        if row["currency"] not in ALLOWED_CURRENCIES:
            errors.append(f"payments: invalid currency {row['currency']}")
        try:
            if parse_decimal(row["amount"]) < 0:
                errors.append("payments: negative amount")
            parse_timestamp(row["payment_timestamp"])
        except (InvalidOperation, ValueError, TypeError):
            errors.append("payments: invalid amount or timestamp")

    returned_by_item = defaultdict(int)

    for row in returns:
        try:
            quantity = int(row["quantity_returned"])
            if quantity <= 0:
                errors.append("returns: quantity_returned must be positive")
            returned_by_item[row["order_item_id"]] += quantity
            refund = parse_decimal(row["refund_amount"])
            if refund < 0:
                errors.append("returns: negative refund amount")
            parse_timestamp(row["return_timestamp"])
        except (ValueError, InvalidOperation, TypeError):
            errors.append("returns: invalid quantity, refund, or timestamp")

    for item_id, returned_quantity in returned_by_item.items():
        item = item_by_id.get(item_id)
        if item:
            try:
                purchased_quantity = int(item["quantity"])
                if returned_quantity > purchased_quantity:
                    errors.append(
                        f"returns: {item_id} returned quantity "
                        f"{returned_quantity} exceeds purchased quantity "
                        f"{purchased_quantity}"
                    )
            except ValueError:
                errors.append(f"order_items: invalid quantity for {item_id}")

    print("=== CommerceLake Source Data Validation ===")
    for name, (rows, _) in datasets.items():
        print(f"{name:12} {len(rows):>7,} rows")

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
