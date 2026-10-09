# Databricks notebook source
# COMMAND ----------
# Cell 1 — Establish the Unity Catalog landing area.
#
# Run this cell first. It creates the schema and volume. Upload the six CSV
# files to the printed volume path before running the ingestion cell below.

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "commerce_lakehouse"
VOLUME = "raw_files"

# Identifiers are discovered from Databricks rather than hard-coded to "main".
if not CATALOG or CATALOG.lower() == "hive_metastore":
    raise RuntimeError(
        f"Expected a Unity Catalog catalog, got {CATALOG!r}. "
        "Select a Unity Catalog catalog and rerun this cell."
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"`{CATALOG}`.`{SCHEMA}`.`{VOLUME}`"
)

SOURCE_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"Upload the six source CSV files to: {SOURCE_DIR}")
print("Do not upload files containing real customer or payment information.")

# COMMAND ----------
# Cell 2 — Validate, type, and ingest all six datasets into Bronze.
#
# This cell validates all inputs before replacing any existing Bronze table.
# Re-running it replaces each table with the current complete source snapshot.

from uuid import uuid4  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

EXPECTED_ROWS = {
    "customers": 1_000,
    "products": 200,
    "orders": 5_000,
    "order_items": 12_489,
    "payments": 5_671,
    "returns": 395,
}

# IDs remain strings to preserve identifiers and leading zeros.
# Money uses DECIMAL(12,2), not floating-point types.
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
        "status": "STRING",
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
        "status": "STRING",
        "amount": "DECIMAL(12,2)",
        "currency": "STRING",
        "payment_method": "STRING",
    },
    "returns": {
        "return_id": "STRING",
        "order_item_id": "STRING",
        "return_timestamp": "TIMESTAMP",
        "returned_quantity": "INT",
        "refund_amount": "DECIMAL(12,2)",
        "reason": "STRING",
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

batch_id = str(uuid4())
staged = {}
source_counts = {}

# Fail before writing if any file is missing.
available_files = {item.name for item in dbutils.fs.ls(SOURCE_DIR)}
missing_files = {
    f"{dataset}.csv" for dataset in COLUMN_TYPES
} - available_files
if missing_files:
    raise FileNotFoundError(
        f"Missing source files in {SOURCE_DIR}: {sorted(missing_files)}"
    )

# Load and validate every dataset before writing any Bronze table.
for dataset, columns in COLUMN_TYPES.items():
    path = f"{SOURCE_DIR}/{dataset}.csv"

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("emptyValue", "")
        .csv(path)
    )

    expected_columns = list(columns)
    if set(raw.columns) != set(expected_columns):
        raise ValueError(
            f"{dataset}: schema drift. "
            f"Expected {sorted(expected_columns)}, "
            f"received {sorted(raw.columns)}"
        )

    source_count = raw.count()
    source_counts[dataset] = source_count
    if source_count != EXPECTED_ROWS[dataset]:
        raise ValueError(
            f"{dataset}: expected {EXPECTED_ROWS[dataset]} rows, "
            f"received {source_count}"
        )

    # Detect non-empty values that cannot be converted to the declared type.
    for column, data_type in columns.items():
        if data_type == "STRING":
            continue
        source_value = F.trim(F.col(column))
        converted_value = source_value.cast(data_type)
        bad_cast = raw.filter(
            source_value.isNotNull()
            & (source_value != "")
            & converted_value.isNull()
        ).limit(1).count()
        if bad_cast:
            raise ValueError(
                f"{dataset}.{column} contains values that cannot be cast "
                f"to {data_type}"
            )

    typed = raw.select(
        *[
            F.col(column).cast(data_type).alias(column)
            for column, data_type in columns.items()
        ]
    )

    primary_key = PRIMARY_KEYS[dataset]
    if typed.filter(
        F.col(primary_key).isNull() | (F.trim(F.col(primary_key)) == "")
    ).limit(1).count():
        raise ValueError(f"{dataset}: null or blank primary key: {primary_key}")

    if (
        typed.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(f"{dataset}: duplicate primary key: {primary_key}")

    staged[dataset] = typed

# Referential integrity: every child key must resolve to a parent.
for child, child_key, parent, parent_key in FOREIGN_KEYS:
    unresolved = (
        staged[child]
        .select(F.col(child_key).alias("_fk"))
        .filter(F.col("_fk").isNotNull())
        .join(
            staged[parent].select(F.col(parent_key).alias("_pk")),
            F.col("_fk") == F.col("_pk"),
            "left_anti",
        )
        .limit(1)
        .count()
    )
    if unresolved:
        raise ValueError(
            f"Foreign-key violation: {child}.{child_key} "
            f"references missing {parent}.{parent_key}"
        )

# Add lineage metadata only after source and relationship checks pass.
# Business columns remain separate from operational metadata.
for dataset, frame in list(staged.items()):
    business_columns = frame.columns
    staged[dataset] = (
        frame
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{dataset}.csv"))
        .withColumn("_source_system", F.lit("commercelake_synthetic"))
        .withColumn("_ingestion_batch_id", F.lit(batch_id))
        .withColumn(
            "_record_hash",
            F.sha2(
                F.to_json(F.struct(*[F.col(c) for c in business_columns])),
                256,
            ),
        )
    )

# Persist managed Delta tables. This is a full-snapshot overwrite strategy:
# it avoids duplicate rows on reruns but does not preserve prior batch history.
for dataset, frame in staged.items():
    table_name = f"{CATALOG}.{SCHEMA}.bronze_{dataset}"
    (
        frame.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# Read back persisted tables to verify the actual target, not just source frames.
results = []
for dataset, expected_count in EXPECTED_ROWS.items():
    table_name = f"{CATALOG}.{SCHEMA}.bronze_{dataset}"
    persisted = spark.table(table_name)
    actual_count = persisted.count()

    required_metadata = {
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_ingestion_batch_id",
        "_record_hash",
    }
    missing_metadata = required_metadata - set(persisted.columns)
    if missing_metadata:
        raise AssertionError(
            f"{table_name}: missing metadata {sorted(missing_metadata)}"
        )
    if actual_count != expected_count:
        raise AssertionError(
            f"{table_name}: expected {expected_count}, got {actual_count}"
        )
    if persisted.filter(F.col("_ingestion_batch_id").isNull()).limit(1).count():
        raise AssertionError(f"{table_name}: null ingestion batch metadata")

    results.append((dataset, table_name, actual_count))

print(f"Ingestion batch: {batch_id}")
print("BRONZE INGESTION RESULTS")
for dataset, table_name, row_count in results:
    print(f"PASS | {dataset:12} | {row_count:>7,} rows | {table_name}")

print("PASS: all six Bronze tables were persisted and verified.")
