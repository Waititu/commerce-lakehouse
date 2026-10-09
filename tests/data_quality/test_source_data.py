import csv
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "raw"

EXPECTED_COUNTS = {
    "customers": 1_000,
    "products": 200,
    "orders": 5_000,
    "order_items": 12_489,
    "payments": 5_671,
    "returns": 395,
}


def load(name):
    with (DATA / f"{name}.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        return list(csv.DictReader(handle))


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    assert parsed.utcoffset() is not None
    return parsed


def test_expected_dataset_volumes():
    for name, expected in EXPECTED_COUNTS.items():
        assert len(load(name)) == expected


def test_primary_keys_are_nonempty_and_unique():
    key_fields = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id",
        "payments": "payment_id",
        "returns": "return_id",
    }

    for dataset, key in key_fields.items():
        rows = load(dataset)
        values = [row[key] for row in rows]
        assert all(values)
        assert len(values) == len(set(values))


def test_all_foreign_keys_resolve():
    customers = {r["customer_id"] for r in load("customers")}
    products = {r["product_id"] for r in load("products")}
    orders = {r["order_id"] for r in load("orders")}
    items = {r["order_item_id"] for r in load("order_items")}

    assert all(r["customer_id"] in customers for r in load("orders"))
    assert all(r["order_id"] in orders for r in load("order_items"))
    assert all(r["product_id"] in products for r in load("order_items"))
    assert all(r["order_id"] in orders for r in load("payments"))
    assert all(r["order_item_id"] in items for r in load("returns"))


def test_required_fields_are_populated():
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

    for dataset, fields in required.items():
        for row in load(dataset):
            assert all(row[field].strip() for field in fields)


def test_quantities_and_money_are_valid():
    for row in load("order_items"):
        assert int(row["quantity"]) > 0
        assert Decimal(row["unit_price"]) >= 0

    for row in load("payments"):
        assert Decimal(row["amount"]) >= 0

    for row in load("returns"):
        assert int(row["quantity_returned"]) > 0
        assert Decimal(row["refund_amount"]) >= 0

    for row in load("products"):
        assert Decimal(row["unit_price"]) >= 0


def test_status_values_are_documented():
    assert {
        r["customer_segment"] for r in load("customers")
    } <= {"standard", "premium", "business"}

    assert {
        r["order_status"] for r in load("orders")
    } <= {"pending", "confirmed", "shipped", "delivered", "cancelled"}

    assert {
        r["payment_status"] for r in load("payments")
    } <= {"initiated", "authorized", "captured", "failed", "refunded"}


def test_timestamps_are_timezone_aware():
    for dataset, field in [
        ("customers", "signup_timestamp"),
        ("orders", "order_timestamp"),
        ("payments", "payment_timestamp"),
        ("returns", "return_timestamp"),
    ]:
        for row in load(dataset):
            timestamp(row[field])


def test_orders_do_not_predate_customer_signup():
    customers = {
        row["customer_id"]: timestamp(row["signup_timestamp"])
        for row in load("customers")
    }

    for order in load("orders"):
        assert timestamp(order["order_timestamp"]) >= customers[
            order["customer_id"]
        ]


def test_returns_respect_purchase_quantity_and_order_lifecycle():
    items = {r["order_item_id"]: r for r in load("order_items")}
    orders = {r["order_id"]: r for r in load("orders")}
    returned = defaultdict(int)

    for row in load("returns"):
        item = items[row["order_item_id"]]
        order = orders[item["order_id"]]

        assert order["order_status"] == "delivered"
        assert timestamp(row["return_timestamp"]) >= timestamp(
            order["order_timestamp"]
        )

        returned[row["order_item_id"]] += int(row["quantity_returned"])

    for item_id, quantity in returned.items():
        assert quantity <= int(items[item_id]["quantity"])


def test_payment_currency_matches_order_currency():
    orders = {r["order_id"]: r for r in load("orders")}

    for payment in load("payments"):
        assert payment["currency"] == orders[payment["order_id"]]["currency"]


def test_captured_payments_reconcile_to_order_item_gross():
    item_totals = defaultdict(Decimal)

    for item in load("order_items"):
        item_totals[item["order_id"]] += (
            Decimal(item["unit_price"]) * int(item["quantity"])
        )

    captured_by_order = defaultdict(list)
    for payment in load("payments"):
        if payment["payment_status"] == "captured":
            captured_by_order[payment["order_id"]].append(
                Decimal(payment["amount"])
            )

    for order in load("orders"):
        captured = captured_by_order[order["order_id"]]

        if order["order_status"] == "cancelled":
            assert not captured
        else:
            assert len(captured) == 1
            assert captured[0] == item_totals[order["order_id"]]


def test_standalone_validator_passes():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "validate_source_data.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS: all source data validation checks succeeded" in result.stdout
