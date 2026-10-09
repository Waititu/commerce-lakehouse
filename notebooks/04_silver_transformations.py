# Databricks notebook source
# CommerceLake — Phase 4: Silver Transformations
#
# Reads managed Bronze Delta tables, standardizes source fields,
# validates row-level and referential rules, quarantines rejected
# records, and writes verified Silver Delta snapshots.
#
# Run after Phase 3 Bronze ingestion has completed successfully.

# COMMAND ----------

from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "commerce_lakehouse"

RUN_ID = str(uuid4())
PROCESSED_AT = F.current_timestamp()

DATASETS = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
)

# COMMAND ----------

# Read the existing Bronze tables.
bronze = {}

for dataset in DATASETS:
    table_name = f"{CATALOG}.{SCHEMA}.bronze_{dataset}"

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required Bronze table does not exist: {table_name}. "
            "Run Phase 3 before Phase 4."
        )

    bronze[dataset] = spark.table(table_name)

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Silver run ID: {RUN_ID}")

for dataset, frame in bronze.items():
    print(f"Bronze {dataset}: {frame.count():,} rows")

# COMMAND ----------

# Standardize names and text values in Silver.
# Bronze tables remain unchanged.

silver_source = {}

for dataset, frame in bronze.items():
    if dataset == "orders":
        frame = frame.withColumnRenamed("order_status", "status")

    elif dataset == "payments":
        frame = frame.withColumnRenamed("payment_status", "status")

    elif dataset == "returns":
        frame = (
            frame
            .withColumnRenamed("quantity_returned", "returned_quantity")
            .withColumnRenamed("return_reason", "reason")
        )

    silver_source[dataset] = frame

# Trim text fields; standardize codes and categorical values.
NORMALIZATIONS = {
    "customers": {
        "country_code": "upper",
        "customer_segment": "lower",
    },
    "products": {
        "product_name": "trim",
        "category": "lower",
        "currency": "upper",
    },
    "orders": {
        "status": "lower",
        "currency": "upper",
    },
    "order_items": {},
    "payments": {
        "status": "lower",
        "currency": "upper",
        "payment_method": "lower",
    },
    "returns": {
        "reason": "lower",
    },
}

for dataset, frame in silver_source.items():
    for column, operation in NORMALIZATIONS[dataset].items():
        expression = F.trim(F.col(column))

        if operation == "upper":
            expression = F.upper(expression)
        elif operation == "lower":
            expression = F.lower(expression)

        frame = frame.withColumn(column, expression)

    silver_source[dataset] = frame

# COMMAND ----------

# Define row-level business rules.
# Each invalid row receives one or more human-readable reasons.

RULES = {
    "customers": [
        (F.col("customer_id").isNull() |
         (F.trim(F.col("customer_id")) == ""), "missing_customer_id"),
        (F.col("signup_timestamp").isNull(), "missing_signup_timestamp"),
        (F.col("country_code").isNull() |
         (F.length("country_code") != 2), "invalid_country_code"),
        (F.col("customer_segment").isNull() |
         (F.col("customer_segment") == ""), "missing_customer_segment"),
    ],
    "products": [
        (F.col("product_id").isNull() |
         (F.trim(F.col("product_id")) == ""), "missing_product_id"),
        (F.col("product_name").isNull() |
         (F.col("product_name") == ""), "missing_product_name"),
        (F.col("category").isNull() |
         (F.col("category") == ""), "missing_category"),
        (F.col("unit_price").isNull() |
         (F.col("unit_price") <= 0), "invalid_unit_price"),
        (F.col("currency").isNull() |
         (F.col("currency") == ""), "missing_currency"),
        (F.col("is_active").isNull(), "missing_is_active"),
    ],
    "orders": [
        (F.col("order_id").isNull() |
         (F.trim(F.col("order_id")) == ""), "missing_order_id"),
        (F.col("customer_id").isNull() |
         (F.trim(F.col("customer_id")) == ""), "missing_customer_id"),
        (F.col("order_timestamp").isNull(), "missing_order_timestamp"),
        (F.col("status").isNull() |
         (F.col("status") == ""), "missing_order_status"),
        (F.col("currency").isNull() |
         (F.col("currency") == ""), "missing_currency"),
    ],
    "order_items": [
        (F.col("order_item_id").isNull() |
         (F.trim(F.col("order_item_id")) == ""), "missing_order_item_id"),
        (F.col("order_id").isNull() |
         (F.trim(F.col("order_id")) == ""), "missing_order_id"),
        (F.col("product_id").isNull() |
         (F.trim(F.col("product_id")) == ""), "missing_product_id"),
        (F.col("quantity").isNull() |
         (F.col("quantity") <= 0), "invalid_quantity"),
        (F.col("unit_price").isNull() |
         (F.col("unit_price") < 0), "invalid_unit_price"),
    ],
    "payments": [
        (F.col("payment_id").isNull() |
         (F.trim(F.col("payment_id")) == ""), "missing_payment_id"),
        (F.col("order_id").isNull() |
         (F.trim(F.col("order_id")) == ""), "missing_order_id"),
        (F.col("payment_timestamp").isNull(), "missing_payment_timestamp"),
        (F.col("status").isNull() |
         (F.col("status") == ""), "missing_payment_status"),
        (F.col("amount").isNull() |
         (F.col("amount") < 0), "invalid_payment_amount"),
        (F.col("currency").isNull() |
         (F.col("currency") == ""), "missing_currency"),
        (F.col("payment_method").isNull() |
         (F.col("payment_method") == ""), "missing_payment_method"),
    ],
    "returns": [
        (F.col("return_id").isNull() |
         (F.trim(F.col("return_id")) == ""), "missing_return_id"),
        (F.col("order_item_id").isNull() |
         (F.trim(F.col("order_item_id")) == ""), "missing_order_item_id"),
        (F.col("return_timestamp").isNull(), "missing_return_timestamp"),
        (F.col("returned_quantity").isNull() |
         (F.col("returned_quantity") <= 0), "invalid_returned_quantity"),
        (F.col("refund_amount").isNull() |
         (F.col("refund_amount") < 0), "invalid_refund_amount"),
        (F.col("reason").isNull() |
         (F.col("reason") == ""), "missing_return_reason"),
    ],
}


def add_rule_reasons(
    frame: DataFrame,
    rules: list,
) -> DataFrame:
    """Attach a semicolon-separated list of failed rule identifiers."""
    reasons = [
        F.when(condition, F.lit(reason))
        for condition, reason in rules
    ]

    return frame.withColumn(
        "_silver_reject_reason",
        F.concat_ws("; ", *reasons),
    )


def split_valid_and_rejected(
    frame: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split a staged frame into accepted and rejected records."""
    rejected = frame.filter(F.col("_silver_reject_reason") != "")
    valid = (
        frame.filter(F.col("_silver_reject_reason") == "")
        .drop("_silver_reject_reason")
    )

    rejected = (
        rejected
        .withColumn("_quarantine_run_id", F.lit(RUN_ID))
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    return valid, rejected


staged = {}
quarantine = {}

for dataset in DATASETS:
    evaluated = add_rule_reasons(
        silver_source[dataset],
        RULES[dataset],
    )

    staged[dataset], quarantine[dataset] = (
        split_valid_and_rejected(evaluated)
    )

# COMMAND ----------

# Enforce referential integrity against accepted parent records.
# Invalid parents are excluded from downstream Silver relationships.

def quarantine_unresolved_foreign_keys(
    child: DataFrame,
    child_key: str,
    parent: DataFrame,
    parent_key: str,
    reason: str,
) -> tuple[DataFrame, DataFrame]:
    """Reject child records whose parent is not in the accepted parent set."""
    parent_keys = parent.select(
        F.col(parent_key).alias("_matched_parent_key")
    )

    joined = child.join(
        parent_keys,
        F.col(child_key) == F.col("_matched_parent_key"),
        "left",
    )

    missing_parent = F.col("_matched_parent_key").isNull()

    evaluated = joined.withColumn(
        "_silver_reject_reason",
        F.when(missing_parent, F.lit(reason)).otherwise(F.lit("")),
    ).drop("_matched_parent_key")

    return split_valid_and_rejected(evaluated)


# Orders must reference accepted customers.
staged["orders"], rejected = quarantine_unresolved_foreign_keys(
    staged["orders"],
    "customer_id",
    staged["customers"],
    "customer_id",
    "unresolved_customer_reference",
)
quarantine["orders"] = quarantine["orders"].unionByName(
    rejected, allowMissingColumns=True
)

# Order items must reference accepted orders and products.
for child_key, parent, parent_key, reason in [
    ("order_id", "orders", "order_id", "unresolved_order_reference"),
    ("product_id", "products", "product_id", "unresolved_product_reference"),
]:
    staged["order_items"], rejected = quarantine_unresolved_foreign_keys(
        staged["order_items"],
        child_key,
        staged[parent],
        parent_key,
        reason,
    )
    quarantine["order_items"] = quarantine["order_items"].unionByName(
        rejected, allowMissingColumns=True
    )

# Payments must reference accepted orders.
staged["payments"], rejected = quarantine_unresolved_foreign_keys(
    staged["payments"],
    "order_id",
    staged["orders"],
    "order_id",
    "unresolved_order_reference",
)
quarantine["payments"] = quarantine["payments"].unionByName(
    rejected, allowMissingColumns=True
)

# Returns must reference accepted order items.
staged["returns"], rejected = quarantine_unresolved_foreign_keys(
    staged["returns"],
    "order_item_id",
    staged["order_items"],
    "order_item_id",
    "unresolved_order_item_reference",
)
quarantine["returns"] = quarantine["returns"].unionByName(
    rejected, allowMissingColumns=True
)

# COMMAND ----------

# A return cannot exceed the purchased quantity for its order item.
purchased_quantities = staged["order_items"].select(
    "order_item_id",
    F.col("quantity").alias("_purchased_quantity"),
)

return_joined = staged["returns"].join(
    purchased_quantities,
    "order_item_id",
    "left",
)

excessive_returns = (
    F.col("returned_quantity") > F.col("_purchased_quantity")
)

return_evaluated = (
    return_joined
    .withColumn(
        "_silver_reject_reason",
        F.when(
            excessive_returns,
            F.lit("returned_quantity_exceeds_purchased_quantity"),
        ).otherwise(F.lit("")),
    )
    .drop("_purchased_quantity")
)

staged["returns"], rejected = split_valid_and_rejected(return_evaluated)

quarantine["returns"] = quarantine["returns"].unionByName(
    rejected, allowMissingColumns=True
)

# COMMAND ----------

# Add Silver processing metadata to accepted rows.
# Preserve existing Bronze lineage fields.

for dataset, frame in list(staged.items()):
    staged[dataset] = (
        frame
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(RUN_ID))
    )

# Record a dataset label and rejection reason for quarantined records.
for dataset, frame in list(quarantine.items()):
    quarantine[dataset] = (
        frame
        .withColumn("_quarantine_dataset", F.lit(dataset))
    )

# COMMAND ----------

# Persist full-snapshot Delta tables.
# Rerunning the notebook replaces each output snapshot.
# These separate writes are not one cross-table atomic transaction.

write_results = []

for dataset in DATASETS:
    silver_table = f"{CATALOG}.{SCHEMA}.silver_{dataset}"
    quarantine_table = f"{CATALOG}.{SCHEMA}.quarantine_{dataset}"

    (
        staged[dataset].write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    (
        quarantine[dataset].write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(quarantine_table)
    )

    write_results.append(
        (dataset, silver_table, quarantine_table)
    )

# COMMAND ----------

# Verify every persisted table and reconcile source row counts.
results = []

for dataset, silver_table, quarantine_table in write_results:
    bronze_count = bronze[dataset].count()
    silver_frame = spark.table(silver_table)
    quarantine_frame = spark.table(quarantine_table)

    silver_count = silver_frame.count()
    quarantine_count = quarantine_frame.count()

    if bronze_count != silver_count + quarantine_count:
        raise AssertionError(
            f"{dataset}: row reconciliation failed. "
            f"Bronze={bronze_count}, Silver={silver_count}, "
            f"Quarantine={quarantine_count}"
        )

    required_silver_metadata = {
        "_silver_processed_at",
        "_silver_run_id",
        "_record_hash",
        "_ingestion_batch_id",
    }

    missing = required_silver_metadata - set(silver_frame.columns)

    if missing:
        raise AssertionError(
            f"{silver_table}: missing metadata {sorted(missing)}"
        )

    required_quarantine_metadata = {
        "_silver_reject_reason",
        "_quarantine_run_id",
        "_quarantined_at",
        "_quarantine_dataset",
    }

    missing = required_quarantine_metadata - set(
        quarantine_frame.columns
    )

    if missing:
        raise AssertionError(
            f"{quarantine_table}: missing metadata {sorted(missing)}"
        )

    if quarantine_count and quarantine_frame.filter(
        F.col("_silver_reject_reason").isNull()
        | (F.col("_silver_reject_reason") == "")
    ).limit(1).count():
        raise AssertionError(
            f"{quarantine_table}: rejected record lacks a reason"
        )

    results.append(
        (dataset, bronze_count, silver_count, quarantine_count)
    )

print(f"Silver run ID: {RUN_ID}")
print("SILVER TRANSFORMATION RESULTS")
print("dataset       bronze      silver   quarantine")

for dataset, bronze_count, silver_count, quarantine_count in results:
    print(
        f"{dataset:12} {bronze_count:>8,} "
        f"{silver_count:>11,} {quarantine_count:>12,}"
    )

print(
    "PASS: all six Silver tables and six quarantine tables "
    "were persisted and reconciled."
)
