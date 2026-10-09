import csv
import random
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

SEED = 20261009
CUSTOMER_COUNT = 1_000
PRODUCT_COUNT = 200
ORDER_COUNT = 5_000

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "raw"

START_DATE = datetime(2023, 1, 1, tzinfo=UTC)
AS_OF = datetime(2026, 9, 30, 23, 59, 59, tzinfo=UTC)
CENT = Decimal("0.01")


def timestamp_between(rng, start, end):
    """Return a reproducible UTC timestamp between two datetimes."""
    seconds = max(0, int((end - start).total_seconds()))
    return start + timedelta(seconds=rng.randint(0, seconds))


def money(value):
    return str(Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP))


def write_csv(filename, rows, columns):
    path = OUTPUT_DIR / filename
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{filename}: {len(rows):,} rows")


def main():
    rng = random.Random(SEED)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    customers = []
    signup_dates = {}

    for i in range(1, CUSTOMER_COUNT + 1):
        customer_id = f"C{i:06d}"
        signup = timestamp_between(rng, START_DATE, AS_OF)
        signup_dates[customer_id] = signup
        customers.append({
            "customer_id": customer_id,
            "signup_timestamp": signup.isoformat().replace("+00:00", "Z"),
            "country_code": rng.choice(["KE", "US", "GB", "CA", "DE"]),
            "customer_segment": rng.choice(
                ["standard", "standard", "standard", "premium", "business"]
            ),
        })

    products = []
    product_prices = {}

    categories = ["electronics", "home", "fashion", "beauty", "sports"]

    for i in range(1, PRODUCT_COUNT + 1):
        product_id = f"P{i:05d}"
        price = Decimal(rng.randint(500, 100_000)) / Decimal(100)
        product_prices[product_id] = price
        products.append({
            "product_id": product_id,
            "product_name": f"Product {i:03d}",
            "category": rng.choice(categories),
            "unit_price": money(price),
            "currency": "USD",
            "is_active": str(rng.random() >= 0.05).lower(),
        })

    orders = []
    order_items = []
    payments = []
    returns = []

    order_statuses = [
        "pending", "confirmed", "shipped", "delivered", "cancelled"
    ]
    order_weights = [5, 10, 15, 60, 10]
    payment_methods = ["card", "mobile_money", "bank_transfer"]
    return_reasons = [
        "damaged", "wrong_item", "not_as_described", "changed_mind"
    ]

    for order_number in range(1, ORDER_COUNT + 1):
        order_id = f"O{order_number:07d}"
        customer = rng.choice(customers)
        customer_id = customer["customer_id"]

        order_time = timestamp_between(
            rng, signup_dates[customer_id], AS_OF
        )
        order_status = rng.choices(
            order_statuses, weights=order_weights, k=1
        )[0]

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_timestamp": order_time.isoformat().replace("+00:00", "Z"),
            "order_status": order_status,
            "currency": "USD",
        })

        order_total = Decimal("0.00")
        item_indexes = []

        for line_number in range(1, rng.randint(1, 4) + 1):
            product_id = rng.choice(list(product_prices))
            catalog_price = product_prices[product_id]
            adjustment = Decimal(rng.randint(90, 110)) / Decimal(100)
            unit_price = (catalog_price * adjustment).quantize(CENT)
            quantity = rng.randint(1, 5)
            line_total = unit_price * quantity
            order_total += line_total

            item_id = f"OI{order_number:07d}-{line_number:02d}"
            item_indexes.append(len(order_items))
            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": money(unit_price),
            })

        payment_method = rng.choice(payment_methods)

        # Cancelled orders do not have a successful captured payment
        # in this initial synthetic source model.
        if order_status == "cancelled":
            payments.append({
                "payment_id": f"PAY{order_number:07d}-01",
                "order_id": order_id,
                "payment_timestamp": order_time.isoformat().replace("+00:00", "Z"),
                "payment_status": rng.choice(["initiated", "failed"]),
                "amount": money(order_total),
                "currency": "USD",
                "payment_method": payment_method,
            })
        else:
            attempt = 1
            if rng.random() < 0.15:
                payments.append({
                    "payment_id": f"PAY{order_number:07d}-{attempt:02d}",
                    "order_id": order_id,
                    "payment_timestamp": order_time.isoformat().replace("+00:00", "Z"),
                    "payment_status": "failed",
                    "amount": money(order_total),
                    "currency": "USD",
                    "payment_method": payment_method,
                })
                attempt += 1

            payments.append({
                "payment_id": f"PAY{order_number:07d}-{attempt:02d}",
                "order_id": order_id,
                "payment_timestamp": order_time.isoformat().replace("+00:00", "Z"),
                "payment_status": "captured",
                "amount": money(order_total),
                "currency": "USD",
                "payment_method": payment_method,
            })

        # Only delivered items are eligible for returns in this fixture.
        if order_status == "delivered":
            for item_index in item_indexes:
                item = order_items[item_index]
                if rng.random() < 0.05:
                    returned_quantity = rng.randint(1, item["quantity"])
                    return_time = timestamp_between(rng, order_time, AS_OF)
                    refund = (
                        Decimal(item["unit_price"]) * returned_quantity
                    ).quantize(CENT)

                    returns.append({
                        "return_id": f"R{len(returns) + 1:07d}",
                        "order_item_id": item["order_item_id"],
                        "return_timestamp": return_time.isoformat().replace("+00:00", "Z"),
                        "quantity_returned": returned_quantity,
                        "refund_amount": money(refund),
                        "return_reason": rng.choice(return_reasons),
                    })

    write_csv("customers.csv", customers, [
        "customer_id", "signup_timestamp", "country_code", "customer_segment"
    ])
    write_csv("products.csv", products, [
        "product_id", "product_name", "category", "unit_price", "currency",
        "is_active"
    ])
    write_csv("orders.csv", orders, [
        "order_id", "customer_id", "order_timestamp", "order_status", "currency"
    ])
    write_csv("order_items.csv", order_items, [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price"
    ])
    write_csv("payments.csv", payments, [
        "payment_id", "order_id", "payment_timestamp", "payment_status",
        "amount", "currency", "payment_method"
    ])
    write_csv("returns.csv", returns, [
        "return_id", "order_item_id", "return_timestamp", "quantity_returned",
        "refund_amount", "return_reason"
    ])

    print(f"\nSeed: {SEED}")
    print(f"Data directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
