from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks" / "06_incremental_bronze.py"


def source_text() -> str:
    return NOTEBOOK.read_text(encoding="utf-8")


def test_all_six_source_datasets_are_declared() -> None:
    source = source_text().split("COLUMN_TYPES = {", 1)[1].split("\n}", 1)[0]
    for dataset in ("customers", "products", "orders", "order_items", "payments", "returns"):
        assert f'"{dataset}": {{' in source


def test_delta_merge_and_hash_based_change_detection_are_used() -> None:
    source = source_text()
    assert "from delta.tables import DeltaTable" in source
    assert ".merge(" in source
    assert "F.sha2(" in source
    assert '"_record_hash"' in source
    assert ".whenMatchedUpdateAll(" in source
    assert ".whenNotMatchedInsertAll()" in source


def test_notebook_does_not_overwrite_or_delete_target_rows() -> None:
    source = source_text()
    assert '.mode("overwrite")' not in source
    assert "whenNotMatchedBySourceDelete" not in source


def test_full_source_validation_occurs_before_merge_loop() -> None:
    source = source_text()
    assert source.index("staged, source_counts = read_and_validate_sources()") < source.index("metrics = merge_dataset(dataset, frame")
    assert "schema drift" in source
    assert "duplicate primary key" in source
    assert "Foreign-key violation" in source


def test_audit_tracks_batch_status_and_operation_counts() -> None:
    source = source_text()
    for value in ("batch_id_override", "incremental_batch_audit", '"STARTED"', '"SUCCESS"', '"FAILED"', '"inserted_rows"', '"updated_rows"', '"unchanged_rows"'):
        assert value in source


def test_target_primary_keys_are_checked_after_merge() -> None:
    source = source_text()
    assert "duplicate target primary key after merge" in source
    assert "null target primary key after merge" in source
