from pathlib import Path

NOTEBOOK = (
    Path(__file__).resolve().parents[2]
    / "notebooks"
    / "05_gold_analytics.py"
)


def notebook_source() -> str:
    return NOTEBOOK.read_text(encoding="utf-8")


def test_gold_notebook_declares_six_models():
    source = notebook_source()

    for model in (
        "gold_daily_sales",
        "gold_customer_360",
        "gold_product_performance",
        "gold_payment_reconciliation",
        "gold_payment_status_daily",
        "gold_returns_summary",
    ):
        assert model in source


def test_gold_notebook_requires_all_silver_inputs():
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

    assert 'f"{CATALOG}.{SCHEMA}.silver_{dataset}"' in source
    assert "REQUIRED_COLUMNS" in source


def test_one_to_many_sources_are_preaggregated_before_joins():
    source = notebook_source()

    assert "order_item_metrics" in source
    assert "payments_by_order" in source
    assert "refunds_by_order" in source
    assert "returns_by_item" in source
    assert "captured_payment_amount" in source


def test_gold_models_have_grain_and_persisted_quality_checks():
    source = notebook_source()

    assert "GOLD_MODELS" in source
    assert "validate_model" in source
    assert "duplicate grain" in source
    assert "_gold_run_id" in source
    assert "_gold_built_at" in source
    assert '.format("delta")' in source
    assert '.mode("overwrite")' in source
    assert "saveAsTable(table_name)" in source


def test_sales_and_payment_metrics_are_distinct():
    source = notebook_source()

    assert "gross_merchandise_value" in source
    assert "captured_to_gmv_difference" in source
    assert "captured_less_refunds" in source
    assert "average_order_gmv" in source
