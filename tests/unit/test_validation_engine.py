"""Regression tests for the reusable CommerceLake validation engine."""

from commerce_lakehouse.contracts import CONTRACTS
from commerce_lakehouse.validation import validate_datasets, validate_headers


def valid_fixture():
    """Return a small, internally consistent six-dataset fixture."""
    datasets = {
        "customers": [{
            "customer_id": "C1",
            "signup_timestamp": "2026-01-01T10:00:00+00:00",
            "country_code": "KE",
            "customer_segment": "standard",
        }],
        "products": [{
            "product_id": "P1",
            "product_name": "Book",
            "category": "books",
            "unit_price": "10.00",
            "currency": "KES",
            "is_active": "true",
        }],
        "orders": [{
            "order_id": "O1",
            "customer_id": "C1",
            "order_timestamp": "2026-01-02T10:00:00+00:00",
            "order_status": "delivered",
            "currency": "KES",
        }],
        "order_items": [{
            "order_item_id": "I1",
            "order_id": "O1",
            "product_id": "P1",
            "quantity": "2",
            "unit_price": "10.00",
        }],
        "payments": [{
            "payment_id": "PAY1",
            "order_id": "O1",
            "payment_timestamp": "2026-01-02T10:05:00+00:00",
            "payment_status": "captured",
            "amount": "20.00",
            "currency": "KES",
            "payment_method": "card",
        }],
        "returns": [],
    }
    headers = {
        name: list(CONTRACTS[name].columns)
        for name in CONTRACTS
    }
    return headers, datasets


def test_valid_fixture_passes_all_contracts():
    headers, datasets = valid_fixture()
    assert validate_datasets(headers, datasets) == []


def test_missing_column_is_reported():
    headers, datasets = valid_fixture()
    headers["customers"].remove("country_code")
    errors = validate_datasets(headers, datasets)
    assert any("customers: missing columns: country_code" in e for e in errors)


def test_unexpected_column_is_reported():
    headers, _datasets = valid_fixture()
    headers["products"].append("unexpected_field")
    errors = validate_headers(headers)
    assert any("products: unexpected columns: unexpected_field" in e for e in errors)


def test_duplicate_primary_key_is_reported():
    headers, datasets = valid_fixture()
    datasets["customers"].append(dict(datasets["customers"][0]))
    errors = validate_datasets(headers, datasets)
    assert any("duplicate customer_id=C1" in e for e in errors)


def test_unknown_foreign_key_is_reported():
    headers, datasets = valid_fixture()
    datasets["orders"][0]["customer_id"] = "DOES_NOT_EXIST"
    errors = validate_datasets(headers, datasets)
    assert any("references unknown customers ID" in e for e in errors)


def test_naive_timestamp_is_rejected():
    headers, datasets = valid_fixture()
    datasets["customers"][0]["signup_timestamp"] = "2026-01-01T10:00:00"
    errors = validate_datasets(headers, datasets)
    assert any("invalid TIMESTAMP" in e for e in errors)


def test_invalid_boolean_is_rejected():
    headers, datasets = valid_fixture()
    datasets["products"][0]["is_active"] = "yes"
    errors = validate_datasets(headers, datasets)
    assert any("is_active must be true or false" in e for e in errors)


def test_excess_decimal_precision_is_rejected():
    headers, datasets = valid_fixture()
    datasets["products"][0]["unit_price"] = "10.001"
    errors = validate_datasets(headers, datasets)
    assert any("invalid DECIMAL(12,2)" in e for e in errors)


def test_payment_currency_must_match_order():
    headers, datasets = valid_fixture()
    datasets["payments"][0]["currency"] = "USD"
    errors = validate_datasets(headers, datasets)
    assert any("currency USD differs from order O1" in e for e in errors)


def test_captured_payment_must_reconcile_to_item_gross():
    headers, datasets = valid_fixture()
    datasets["payments"][0]["amount"] = "19.00"
    errors = validate_datasets(headers, datasets)
    assert any("does not equal item gross" in e for e in errors)


def test_cumulative_returns_cannot_exceed_purchased_quantity():
    headers, datasets = valid_fixture()
    datasets["returns"] = [
        {
            "return_id": "R1",
            "order_item_id": "I1",
            "return_timestamp": "2026-01-03T10:00:00+00:00",
            "quantity_returned": "2",
            "refund_amount": "20.00",
            "return_reason": "damaged",
        },
        {
            "return_id": "R2",
            "order_item_id": "I1",
            "return_timestamp": "2026-01-04T10:00:00+00:00",
            "quantity_returned": "1",
            "refund_amount": "10.00",
            "return_reason": "damaged",
        },
    ]
    errors = validate_datasets(headers, datasets)
    assert any("returned quantity 3 exceeds purchased quantity 2" in e for e in errors)


def test_cancelled_order_cannot_have_captured_payment():
    headers, datasets = valid_fixture()
    datasets["orders"][0]["order_status"] = "cancelled"
    errors = validate_datasets(headers, datasets)
    assert any("cancelled order O1 has captured payment" in e for e in errors)


def test_invalid_status_is_rejected():
    headers, datasets = valid_fixture()
    datasets["orders"][0]["order_status"] = "mystery"
    errors = validate_datasets(headers, datasets)
    assert any("invalid order_status='mystery'" in e for e in errors)


def test_orders_cannot_predate_customer_signup():
    headers, datasets = valid_fixture()
    datasets["orders"][0]["order_timestamp"] = "2025-12-31T10:00:00+00:00"
    errors = validate_datasets(headers, datasets)
    assert any("predates customer signup" in e for e in errors)


def test_short_csv_rows_report_missing_cells(tmp_path):
    from commerce_lakehouse.validation import load_csv_datasets

    customer_columns = list(CONTRACTS["customers"].columns)
    customer_file = tmp_path / "customers.csv"
    customer_file.write_text(
        ",".join(customer_columns)
        + "\nC1,2026-01-01T10:00:00+00:00\n",
        encoding="utf-8",
    )

    headers, datasets = load_csv_datasets(tmp_path)
    errors = validate_datasets(headers, datasets)

    assert any(
        "customers CSV line 2: missing CSV value(s)" in error
        and "country_code" in error
        for error in errors
    )


def test_long_csv_rows_report_extra_fields(tmp_path):
    from commerce_lakehouse.validation import load_csv_datasets

    customer_columns = list(CONTRACTS["customers"].columns)
    customer_file = tmp_path / "customers.csv"
    customer_file.write_text(
        ",".join(customer_columns)
        + "\nC1,2026-01-01T10:00:00+00:00,KE,standard,EXTRA\n",
        encoding="utf-8",
    )

    headers, datasets = load_csv_datasets(tmp_path)
    errors = validate_datasets(headers, datasets)

    assert any(
        "customers CSV line 2: 1 unexpected extra CSV field(s)" in error
        for error in errors
    )


def test_none_values_do_not_crash_validation():
    headers, datasets = valid_fixture()
    datasets["customers"][0]["country_code"] = None

    errors = validate_datasets(headers, datasets)

    assert any("missing CSV value(s)" in error for error in errors)
    assert any("country_code is empty" in error for error in errors)
