from pathlib import Path

NOTEBOOK = (
    Path(__file__).resolve().parents[2]
    / "notebooks"
    / "04_silver_transformations.py"
)


def notebook_source() -> str:
    return NOTEBOOK.read_text(encoding="utf-8")


def test_silver_notebook_covers_all_six_datasets():
    source = notebook_source()

    for dataset in (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "returns",
    ):
        assert f'"{dataset}"' in source

    assert 'f"{CATALOG}.{SCHEMA}.bronze_{dataset}"' in source
    assert 'f"{CATALOG}.{SCHEMA}.silver_{dataset}"' in source
    assert 'f"{CATALOG}.{SCHEMA}.quarantine_{dataset}"' in source


def test_source_fields_are_standardized_without_changing_bronze():
    source = notebook_source()

    assert 'withColumnRenamed("order_status", "status")' in source
    assert 'withColumnRenamed("payment_status", "status")' in source
    assert (
        'withColumnRenamed("quantity_returned", "returned_quantity")'
        in source
    )
    assert 'withColumnRenamed("return_reason", "reason")' in source
    assert "spark.table(table_name)" in source


def test_quality_rules_and_quarantine_are_present():
    source = notebook_source()

    for rule in (
        "invalid_unit_price",
        "invalid_quantity",
        "invalid_payment_amount",
        "invalid_refund_amount",
        "unresolved_customer_reference",
        "unresolved_order_reference",
        "unresolved_product_reference",
        "unresolved_order_item_reference",
        "returned_quantity_exceeds_purchased_quantity",
    ):
        assert rule in source

    assert "_silver_reject_reason" in source
    assert "_quarantined_at" in source
    assert "_quarantine_run_id" in source


def test_silver_lineage_and_row_reconciliation_are_present():
    source = notebook_source()

    assert "_silver_processed_at" in source
    assert "_silver_run_id" in source
    assert "_record_hash" in source
    assert "_ingestion_batch_id" in source
    assert "bronze_count != silver_count + quarantine_count" in source
    assert '.format("delta")' in source
    assert '.mode("overwrite")' in source
