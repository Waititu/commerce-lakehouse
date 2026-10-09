# Databricks notebook source
# MAGIC %md
# MAGIC # CommerceLake — Incremental Bronze Ingestion
# MAGIC
# MAGIC Validates all six source CSVs before applying Delta Lake MERGE operations.
# MAGIC New keys are inserted, changed business records are updated by hash, and
# MAGIC unchanged records are not rewritten. Source-absent target rows are retained.
# MAGIC Silver and Gold are not refreshed by this notebook.
# COMMAND ----------
from datetime import UTC, datetime
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "commerce_lakehouse"
VOLUME = "raw_files"

if not CATALOG or CATALOG.lower() == "hive_metastore":
    raise RuntimeError("Select a Unity Catalog catalog before running this notebook.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
SOURCE_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.incremental_batch_audit"

dbutils.widgets.text("batch_id_override", "")
BATCH_ID = dbutils.widgets.get("batch_id_override").strip() or str(uuid4())
RUN_STARTED_AT = datetime.now(UTC).replace(tzinfo=None)

# These are source CSV headers. Silver is responsible for standardizing names.
COLUMN_TYPES = {
    "customers": {
        "customer_id": "STRING",
        "signup_timestamp": "TIMESTAMP",
        "country_code": "STRING",
        "customer_segment": "STRING",
    },
    "products": {
        "product_id": "STRING",
        "product_name": "STRING",
        "category": "STRING",
        "unit_price": "DECIMAL(12,2)",
        "currency": "STRING",
        "is_active": "BOOLEAN",
    },
    "orders": {
        "order_id": "STRING",
        "customer_id": "STRING",
        "order_timestamp": "TIMESTAMP",
        "order_status": "STRING",
        "currency": "STRING",
    },
    "order_items": {
        "order_item_id": "STRING",
        "order_id": "STRING",
        "product_id": "STRING",
        "quantity": "INT",
        "unit_price": "DECIMAL(12,2)",
    },
    "payments": {
        "payment_id": "STRING",
        "order_id": "STRING",
        "payment_timestamp": "TIMESTAMP",
        "payment_status": "STRING",
        "amount": "DECIMAL(12,2)",
        "currency": "STRING",
        "payment_method": "STRING",
    },
    "returns": {
        "return_id": "STRING",
        "order_item_id": "STRING",
        "return_timestamp": "TIMESTAMP",
        "quantity_returned": "INT",
        "refund_amount": "DECIMAL(12,2)",
        "return_reason": "STRING",
    },
}

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "returns": "return_id",
}

FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("returns", "order_item_id", "order_items", "order_item_id"),
]
LINEAGE_COLUMNS = [
    "_ingested_at", "_source_file", "_source_system",
    "_ingestion_batch_id", "_record_hash",
]

# COMMAND ----------
def bronze_table(dataset: str) -> str:
    return f"{CATALOG}.{SCHEMA}.bronze_{dataset}"


def read_and_validate_sources() -> tuple[dict[str, DataFrame], dict[str, int]]:
    """Stage and validate the entire input snapshot before any target mutation."""
    staged: dict[str, DataFrame] = {}
    source_counts: dict[str, int] = {}

    for dataset, columns in COLUMN_TYPES.items():
        path = f"{SOURCE_DIR}/{dataset}.csv"
        raw = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .option("emptyValue", "")
            .csv(path)
        )
        expected_columns = list(columns)
        if set(raw.columns) != set(expected_columns):
            raise ValueError(
                f"{dataset}: schema drift. Expected {sorted(expected_columns)}, "
                f"received {sorted(raw.columns)}"
            )

        source_counts[dataset] = raw.count()
        for column, data_type in columns.items():
            if data_type == "STRING":
                continue
            value = F.trim(F.col(column))
            cast_value = value.cast(data_type)
            if raw.filter(
                value.isNotNull() & (value != "") & cast_value.isNull()
            ).limit(1).count():
                raise ValueError(
                    f"{dataset}.{column} contains values that cannot be cast to {data_type}"
                )

        typed = raw.select(
            *[F.col(column).cast(data_type).alias(column)
              for column, data_type in columns.items()]
        )
        pk = PRIMARY_KEYS[dataset]
        if typed.filter(
            F.col(pk).isNull() | (F.trim(F.col(pk)) == "")
        ).limit(1).count():
            raise ValueError(f"{dataset}: null or blank primary key: {pk}")
        if typed.groupBy(pk).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"{dataset}: duplicate primary key: {pk}")
        staged[dataset] = typed

    # Validate relationships across the same incoming source snapshot.
    for child, child_key, parent, parent_key in FOREIGN_KEYS:
        unresolved = (
            staged[child].select(F.col(child_key).alias("_fk"))
            .filter(F.col("_fk").isNotNull())
            .join(
                staged[parent].select(F.col(parent_key).alias("_pk")),
                F.col("_fk") == F.col("_pk"),
                "left_anti",
            )
            .limit(1).count()
        )
        if unresolved:
            raise ValueError(
                f"Foreign-key violation: {child}.{child_key} references "
                f"missing {parent}.{parent_key}"
            )
    return staged, source_counts


AUDIT_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("dataset", StringType(), False),
    StructField("status", StringType(), False),
    StructField("source_rows", LongType(), False),
    StructField("inserted_rows", LongType(), False),
    StructField("updated_rows", LongType(), False),
    StructField("unchanged_rows", LongType(), False),
    StructField("target_rows_after", LongType(), False),
    StructField("error_message", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), True),
])


def ensure_audit_table() -> None:
    if not spark.catalog.tableExists(AUDIT_TABLE):
        (spark.createDataFrame([], AUDIT_SCHEMA).write.format("delta")
         .mode("errorifexists").saveAsTable(AUDIT_TABLE))


def upsert_audit(
    dataset: str, status: str, source_rows: int, inserted_rows: int = 0,
    updated_rows: int = 0, unchanged_rows: int = 0, target_rows_after: int = 0,
    error_message: str | None = None,
) -> None:
    completed = (
        datetime.now(UTC).replace(tzinfo=None)
        if status in {"SUCCESS", "FAILED"} else None
    )
    record = {
        "batch_id": BATCH_ID, "dataset": dataset, "status": status,
        "source_rows": int(source_rows), "inserted_rows": int(inserted_rows),
        "updated_rows": int(updated_rows), "unchanged_rows": int(unchanged_rows),
        "target_rows_after": int(target_rows_after),
        "error_message": error_message, "started_at": RUN_STARTED_AT,
        "completed_at": completed,
    }
    source = spark.createDataFrame([record], schema=AUDIT_SCHEMA)
    (DeltaTable.forName(spark, AUDIT_TABLE).alias("target")
     .merge(source.alias("source"),
            "target.batch_id = source.batch_id AND target.dataset = source.dataset")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def merge_dataset(dataset: str, frame: DataFrame, source_rows: int) -> dict[str, int]:
    """Insert new keys, update changed hashes, and leave unchanged rows untouched."""
    name = bronze_table(dataset)
    pk = PRIMARY_KEYS[dataset]
    business_columns = frame.columns
    prepared = (
        frame.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{dataset}.csv"))
        .withColumn("_source_system", F.lit("commercelake_synthetic"))
        .withColumn("_ingestion_batch_id", F.lit(BATCH_ID))
        .withColumn("_record_hash", F.sha2(
            F.to_json(F.struct(*[F.col(c) for c in business_columns])), 256
        ))
    )

    if not spark.catalog.tableExists(name):
        prepared.write.format("delta").mode("errorifexists").saveAsTable(name)
        target_rows = spark.table(name).count()
        if target_rows != source_rows:
            raise RuntimeError(
                f"{dataset}: initial target count {target_rows} != source {source_rows}"
            )
        return {"inserted": source_rows, "updated": 0, "unchanged": 0,
                "target_rows": target_rows}

    target = spark.table(name)
    expected = set(business_columns + LINEAGE_COLUMNS)
    if set(target.columns) != expected:
        raise ValueError(
            f"{dataset}: target schema drift. Expected {sorted(expected)}, "
            f"received {sorted(target.columns)}"
        )

    # Counts assume one writer per catalog/schema. MERGE remains safe to replay;
    # simultaneous writers are not supported by this runbook.
    source_hashes = prepared.select(pk, "_record_hash").alias("s")
    target_hashes = target.select(pk, "_record_hash").alias("t")
    joined = source_hashes.join(
        target_hashes, F.col(f"s.`{pk}`") == F.col(f"t.`{pk}`"), "left"
    )
    metric_row = joined.agg(
        F.sum(F.when(F.col(f"t.`{pk}`").isNull(), 1).otherwise(0)).alias("inserted"),
        F.sum(F.when(
            F.col(f"t.`{pk}`").isNotNull()
            & ~F.col("s._record_hash").eqNullSafe(F.col("t._record_hash")), 1
        ).otherwise(0)).alias("updated"),
        F.sum(F.when(
            F.col(f"t.`{pk}`").isNotNull()
            & F.col("s._record_hash").eqNullSafe(F.col("t._record_hash")), 1
        ).otherwise(0)).alias("unchanged"),
    ).first()
    inserted = int(metric_row["inserted"] or 0)
    updated = int(metric_row["updated"] or 0)
    unchanged = int(metric_row["unchanged"] or 0)

    (DeltaTable.forName(spark, name).alias("target")
     .merge(prepared.alias("source"), f"target.`{pk}` = source.`{pk}`")
     .whenMatchedUpdateAll(
         condition="NOT (target.`_record_hash` <=> source.`_record_hash`)"
     )
     .whenNotMatchedInsertAll().execute())

    persisted = spark.table(name)
    target_rows = persisted.count()
    if persisted.groupBy(pk).count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError(f"{dataset}: duplicate target primary key after merge")
    if persisted.filter(F.col(pk).isNull()).limit(1).count():
        raise RuntimeError(f"{dataset}: null target primary key after merge")
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged,
            "target_rows": target_rows}

# COMMAND ----------
ensure_audit_table()
staged, source_counts = read_and_validate_sources()

# Persist STARTED for every dataset before the first data-table mutation.
for dataset in COLUMN_TYPES:
    upsert_audit(dataset, "STARTED", source_counts[dataset])

for dataset, frame in staged.items():
    try:
        metrics = merge_dataset(dataset, frame, source_counts[dataset])
        upsert_audit(
            dataset, "SUCCESS", source_counts[dataset],
            inserted_rows=metrics["inserted"], updated_rows=metrics["updated"],
            unchanged_rows=metrics["unchanged"],
            target_rows_after=metrics["target_rows"],
        )
        print(
            f"PASS {dataset}: source={source_counts[dataset]}, "
            f"inserted={metrics['inserted']}, updated={metrics['updated']}, "
            f"unchanged={metrics['unchanged']}, target={metrics['target_rows']}"
        )
    except Exception as exc:
        upsert_audit(
            dataset, "FAILED", source_counts[dataset], error_message=str(exc)[:4000]
        )
        raise

print(f"Incremental Bronze batch: {BATCH_ID}")
print(f"Audit table: {AUDIT_TABLE}")
display(spark.table(AUDIT_TABLE).filter(F.col("batch_id") == BATCH_ID).orderBy("dataset"))

batch_audit = spark.table(AUDIT_TABLE).filter(F.col("batch_id") == BATCH_ID)
if batch_audit.filter(F.col("status") != "SUCCESS").limit(1).count():
    raise RuntimeError(f"Batch {BATCH_ID} did not complete successfully")
if batch_audit.select("dataset").distinct().count() != len(COLUMN_TYPES):
    raise RuntimeError(f"Batch {BATCH_ID} has incomplete audit records")
print("PASS: all six incremental Bronze merges and audit checks completed.")
