"""Tests for CommerceLake's canonical source contracts."""

from commerce_lakehouse.contracts import CONTRACTS, validate_contract_definitions


def test_all_six_source_datasets_have_contracts():
    assert set(CONTRACTS) == {
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "returns",
    }


def test_contract_definitions_are_internally_consistent():
    assert validate_contract_definitions() == []


def test_each_contract_declares_a_valid_primary_key():
    for contract in CONTRACTS.values():
        assert contract.primary_key in contract.columns
        assert contract.primary_key in contract.required_fields


def test_each_contract_declares_only_known_required_columns():
    for contract in CONTRACTS.values():
        assert set(contract.required_fields) <= set(contract.columns)


def test_each_foreign_key_targets_a_declared_parent_column():
    for contract in CONTRACTS.values():
        for relationship in contract.foreign_keys:
            parent = CONTRACTS[relationship.parent_dataset]
            assert relationship.field in contract.columns
            assert relationship.parent_field in parent.columns


def test_source_column_order_matches_documented_csv_schemas():
    expected = {
        "customers": [
            "customer_id", "signup_timestamp", "country_code", "customer_segment"
        ],
        "products": [
            "product_id", "product_name", "category", "unit_price", "currency",
            "is_active",
        ],
        "orders": [
            "order_id", "customer_id", "order_timestamp", "order_status", "currency"
        ],
        "order_items": [
            "order_item_id", "order_id", "product_id", "quantity", "unit_price"
        ],
        "payments": [
            "payment_id", "order_id", "payment_timestamp", "payment_status",
            "amount", "currency", "payment_method",
        ],
        "returns": [
            "return_id", "order_item_id", "return_timestamp", "quantity_returned",
            "refund_amount", "return_reason",
        ],
    }

    for name, columns in expected.items():
        assert list(CONTRACTS[name].columns) == columns
