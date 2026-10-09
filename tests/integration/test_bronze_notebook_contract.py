"""Static contract checks for the Databricks notebook source.

These tests do not execute Spark or claim to replace a real Databricks run.
They catch accidental removal of the Phase 3 datasets and safety checks.
"""

from pathlib import Path

NOTEBOOK = (
    Path(__file__).resolve().parents[2]
    / "notebooks"
    / "03_bronze_ingestion.py"
)


def test_bronze_notebook_declares_all_six_datasets() -> None:
    source = NOTEBOOK.read_text(encoding="utf-8")
    assert 'f"{dataset}.csv"' in source
    for dataset in (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "returns",
    ):
        assert f'"{dataset}"' in source


def test_bronze_notebook_has_explicit_types_and_lineage() -> None:
    source = NOTEBOOK.read_text(encoding="utf-8")
    for required in (
        '"DECIMAL(12,2)"',
        '"TIMESTAMP"',
        '"_ingested_at"',
        '"_source_file"',
        '"_source_system"',
        '"_ingestion_batch_id"',
        '"_record_hash"',
    ):
        assert required in source


def test_bronze_notebook_validates_keys_and_persists_delta() -> None:
    source = NOTEBOOK.read_text(encoding="utf-8")
    assert "PRIMARY_KEYS" in source
    assert "FOREIGN_KEYS" in source
    assert "left_anti" in source
    assert '.format("delta")' in source
    assert ".saveAsTable(table_name)" in source
    assert '.mode("overwrite")' in source
