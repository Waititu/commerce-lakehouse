# Databricks notebook source
# CommerceLake — Phase 5: Gold Analytics Models
#
# Builds business-facing managed Delta tables from validated Silver tables.
# Each output has a documented grain and is independently validated.
# This notebook uses Databricks-provided `spark`.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "commerce_lakehouse"

RUN_ID = __import__("uuid").uuid4().hex
ZERO_MONEY = F.lit(0).cast("DECIMAL(18,2)")

DATASETS = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
)

# COMMAND ----------

# Fail before doing transformations if a required Silver input is missing.
silver = {}

for dataset in DATASETS:
    table_name = f"{CATALOG}.{SCHEMA}.silver_{dataset}"

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required Silver table is missing: {table_name}. "
            "Run Phase 4 before Phase 5."
        )

    silver[dataset] = spark.table(table_name)

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Gold run ID: {RUN_ID}")

# COMMAND ----------

# Validate the contracts needed by this notebook.
REQUIRED_COLUMNS = {
    "customers": {
        "customer_id",
        "signup_timestamp",
        "country_code",
        "customer_segment",
    },
    "products": {
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "currency",
        "is_active",
    },
    "orders": {
        "order_id",
        "customer_id",
        "order_timestamp",
        "status",
        "currency",
    },
    "order_items": {
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    },
    "payments": {
        "payment_id",
        "order_id",
        "payment_timestamp",
        "status",
        "amount",
        "currency",
        "payment_method",
    },
    "returns": {
        "return_id",
        "order_item_id",
        "return_timestamp",
        "returned_quantity",
        "refund_amount",
        "reason",
    },
}

for dataset, required in REQUIRED_COLUMNS.items():
    missing = required - set(silver[dataset].columns)
    if missing:
        raise RuntimeError(
            f"Silver {dataset} is missing required columns: "
            f"{sorted(missing)}"
        )

# COMMAND ----------

# Aggregate each one-to-many relationship before joining it to another.
# This prevents order-item × payment × return fan-out from inflating metrics.

order_item_metrics = (
    silver["order_items"]
    .groupBy("order_id")
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("gross_merchandise_value"),
    )
)

# COMMAND ----------

# Gold 1: Daily sales.
# Grain: one row per order date, currency, and order status.
# GMV is item quantity × item unit price, not captured payment revenue.

orders_for_sales = (
    silver["orders"]
    .select(
        "order_id",
        "order_timestamp",
        "status",
        "currency",
    )
    .join(order_item_metrics, "order_id", "left")
    .select(
        F.to_date("order_timestamp").alias("order_date"),
        "currency",
        F.coalesce(F.col("status"), F.lit("unknown")).alias(
            "order_status"
        ),
        "order_id",
        F.coalesce(
            F.col("order_item_count"), F.lit(0)
        ).alias("order_item_count"),
        F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
        F.coalesce(
            F.col("gross_merchandise_value"), ZERO_MONEY
        ).alias("gross_merchandise_value"),
    )
)

gold_daily_sales = (
    orders_for_sales
    .groupBy("order_date", "currency", "order_status")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("order_item_count").alias("order_item_count"),
        F.sum("units_sold").alias("units_sold"),
        F.sum("gross_merchandise_value").alias(
            "gross_merchandise_value"
        ),
        F.avg("gross_merchandise_value").alias("average_order_gmv"),
    )
)

# COMMAND ----------

# Gold 2: Customer 360.
# Grain: one row per customer, including customers with no orders.
# Lifetime value here means cumulative order-item GMV, not profit.

customer_order_metrics = (
    silver["orders"]
    .select(
        "order_id",
        "customer_id",
        "order_timestamp",
    )
    .join(order_item_metrics, "order_id", "left")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum(
            F.coalesce(F.col("order_item_count"), F.lit(0))
        ).alias("order_item_count"),
        F.sum(
            F.coalesce(F.col("units_sold"), F.lit(0))
        ).alias("units_sold"),
        F.sum(
            F.coalesce(
                F.col("gross_merchandise_value"), ZERO_MONEY
            )
        ).alias("lifetime_gmv"),
        F.avg("gross_merchandise_value").alias("average_order_gmv"),
        F.min("order_timestamp").alias("first_order_timestamp"),
        F.max("order_timestamp").alias("last_order_timestamp"),
    )
)

gold_customer_360 = (
    silver["customers"]
    .select(
        "customer_id",
        "signup_timestamp",
        "country_code",
        "customer_segment",
    )
    .join(customer_order_metrics, "customer_id", "left")
    .select(
        "customer_id",
        "signup_timestamp",
        "country_code",
        "customer_segment",
        F.coalesce(F.col("order_count"), F.lit(0)).alias(
            "order_count"
        ),
        F.coalesce(F.col("order_item_count"), F.lit(0)).alias(
            "order_item_count"
        ),
        F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
        F.coalesce(F.col("lifetime_gmv"), ZERO_MONEY).alias(
            "lifetime_gmv"
        ),
        "average_order_gmv",
        "first_order_timestamp",
        "last_order_timestamp",
    )
)

# COMMAND ----------

# Aggregate returns to order-item grain before joining to sales lines.
returns_by_item = (
    silver["returns"]
    .groupBy("order_item_id")
    .agg(
        F.sum("returned_quantity").alias("returned_quantity"),
        F.sum("refund_amount").alias("refund_amount"),
        F.countDistinct("return_id").alias("return_event_count"),
    )
)

# Gold 3: Product performance.
# Grain: one row per product.
# Returned units and refunds are aggregated independently per order item.

items_with_returns = (
    silver["order_items"]
    .join(returns_by_item, "order_item_id", "left")
    .select(
        "product_id",
        "quantity",
        (
            F.col("quantity") * F.col("unit_price")
        ).alias("line_gmv"),
        F.coalesce(
            F.col("returned_quantity"), F.lit(0)
        ).alias("returned_quantity"),
        F.coalesce(F.col("refund_amount"), ZERO_MONEY).alias(
            "refund_amount"
        ),
        F.coalesce(
            F.col("return_event_count"), F.lit(0)
        ).alias("return_event_count"),
    )
)

product_metrics = (
    items_with_returns
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_gmv").alias("gross_merchandise_value"),
        F.sum("returned_quantity").alias("units_returned"),
        F.sum("refund_amount").alias("refund_amount"),
        F.sum("return_event_count").alias("return_event_count"),
    )
)

gold_product_performance = (
    silver["products"]
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "currency",
        "is_active",
    )
    .join(product_metrics, "product_id", "left")
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "currency",
        "is_active",
        F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
        F.coalesce(
            F.col("gross_merchandise_value"), ZERO_MONEY
        ).alias("gross_merchandise_value"),
        F.coalesce(F.col("units_returned"), F.lit(0)).alias(
            "units_returned"
        ),
        F.coalesce(F.col("refund_amount"), ZERO_MONEY).alias(
            "refund_amount"
        ),
        F.coalesce(F.col("return_event_count"), F.lit(0)).alias(
            "return_event_count"
        ),
    )
    .withColumn(
        "net_units_sold",
        F.col("units_sold") - F.col("units_returned"),
    )
    .withColumn(
        "gmv_after_refunds",
        F.col("gross_merchandise_value") - F.col("refund_amount"),
    )
)

# COMMAND ----------

# Aggregate payments and returns to order grain before reconciliation.
payments_by_order = (
    silver["payments"]
    .groupBy("order_id")
    .agg(
        F.countDistinct("payment_id").alias("payment_count"),
        F.sum("amount").alias("total_payment_amount"),
        F.sum(
            F.when(
                F.lower(F.col("status")) == "captured",
                F.col("amount"),
            ).otherwise(ZERO_MONEY)
        ).alias("captured_payment_amount"),
    )
)

returns_with_order = (
    silver["returns"]
    .join(
        silver["order_items"].select("order_item_id", "order_id"),
        "order_item_id",
        "inner",
    )
)

refunds_by_order = (
    returns_with_order
    .groupBy("order_id")
    .agg(
        F.sum("refund_amount").alias("total_refunds"),
        F.sum("returned_quantity").alias("returned_quantity"),
        F.countDistinct("return_id").alias("return_event_count"),
    )
)

# Gold 4: Payment reconciliation.
# Grain: one row per order. A missing payment or refund is represented as zero.

gold_payment_reconciliation = (
    silver["orders"]
    .select("order_id", "customer_id", "order_timestamp", "status", "currency")
    .join(order_item_metrics, "order_id", "left")
    .join(payments_by_order, "order_id", "left")
    .join(refunds_by_order, "order_id", "left")
    .select(
        "order_id",
        "customer_id",
        "order_timestamp",
        F.col("status").alias("order_status"),
        "currency",
        F.coalesce(
            F.col("gross_merchandise_value"), ZERO_MONEY
        ).alias("gross_merchandise_value"),
        F.coalesce(F.col("payment_count"), F.lit(0)).alias(
            "payment_count"
        ),
        F.coalesce(
            F.col("total_payment_amount"), ZERO_MONEY
        ).alias("total_payment_amount"),
        F.coalesce(
            F.col("captured_payment_amount"), ZERO_MONEY
        ).alias("captured_payment_amount"),
        F.coalesce(F.col("total_refunds"), ZERO_MONEY).alias(
            "total_refunds"
        ),
        F.coalesce(F.col("returned_quantity"), F.lit(0)).alias(
            "returned_quantity"
        ),
        F.coalesce(F.col("return_event_count"), F.lit(0)).alias(
            "return_event_count"
        ),
    )
    .withColumn(
        "captured_less_refunds",
        F.col("captured_payment_amount") - F.col("total_refunds"),
    )
    .withColumn(
        "captured_to_gmv_difference",
        F.col("captured_payment_amount")
        - F.col("gross_merchandise_value"),
    )
)

# COMMAND ----------

# Gold 5: Daily payment status.
# Grain: date, payment status, currency, and payment method.

gold_payment_status_daily = (
    silver["payments"]
    .select(
        F.to_date("payment_timestamp").alias("payment_date"),
        F.coalesce(F.lower(F.col("status")), F.lit("unknown")).alias(
            "payment_status"
        ),
        F.coalesce(F.upper(F.col("currency")), F.lit("unknown")).alias(
            "currency"
        ),
        F.coalesce(F.lower(F.col("payment_method")), F.lit("unknown")).alias(
            "payment_method"
        ),
        "payment_id",
        "amount",
    )
    .groupBy(
        "payment_date",
        "payment_status",
        "currency",
        "payment_method",
    )
    .agg(
        F.countDistinct("payment_id").alias("payment_count"),
        F.sum("amount").alias("payment_amount"),
        F.avg("amount").alias("average_payment_amount"),
    )
)

# COMMAND ----------

# Gold 6: Returns summary.
# Grain: return date, reason, product category, and order currency.
# Each return event joins to one order item, product, and order.

gold_returns_summary = (
    silver["returns"]
    .join(
        silver["order_items"].select(
            "order_item_id", "order_id", "product_id"
        ),
        "order_item_id",
        "inner",
    )
    .join(
        silver["products"].select("product_id", "category"),
        "product_id",
        "inner",
    )
    .join(
        silver["orders"].select("order_id", "currency"),
        "order_id",
        "inner",
    )
    .select(
        F.to_date("return_timestamp").alias("return_date"),
        F.coalesce(F.lower(F.col("reason")), F.lit("unknown")).alias(
            "reason"
        ),
        F.coalesce(F.lower(F.col("category")), F.lit("unknown")).alias(
            "category"
        ),
        F.upper("currency").alias("currency"),
        "return_id",
        "returned_quantity",
        "refund_amount",
    )
    .groupBy("return_date", "reason", "category", "currency")
    .agg(
        F.countDistinct("return_id").alias("return_event_count"),
        F.sum("returned_quantity").alias("returned_quantity"),
        F.sum("refund_amount").alias("refund_amount"),
    )
)

# COMMAND ----------

# Write each Gold model as a managed Delta table.
# These are separate table transactions, not one cross-table atomic commit.

GOLD_MODELS = {
    "gold_daily_sales": (
        gold_daily_sales,
        ["order_date", "currency", "order_status"],
    ),
    "gold_customer_360": (
        gold_customer_360,
        ["customer_id"],
    ),
    "gold_product_performance": (
        gold_product_performance,
        ["product_id"],
    ),
    "gold_payment_reconciliation": (
        gold_payment_reconciliation,
        ["order_id"],
    ),
    "gold_payment_status_daily": (
        gold_payment_status_daily,
        [
            "payment_date",
            "payment_status",
            "currency",
            "payment_method",
        ],
    ),
    "gold_returns_summary": (
        gold_returns_summary,
        ["return_date", "reason", "category", "currency"],
    ),
}


def validate_model(
    frame: DataFrame,
    table_name: str,
    key_columns: list[str],
) -> int:
    """Validate key nullability and uniqueness; return row count."""
    for column in key_columns:
        if frame.filter(F.col(column).isNull()).limit(1).count():
            raise AssertionError(
                f"{table_name}: null value in key column {column}"
            )

    duplicates = (
        frame.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise AssertionError(
            f"{table_name}: duplicate grain for {key_columns}"
        )

    return frame.count()


results = []

for model_name, (frame, key_columns) in GOLD_MODELS.items():
    table_name = f"{CATALOG}.{SCHEMA}.{model_name}"

    (
        frame
        .withColumn("_gold_run_id", F.lit(RUN_ID))
        .withColumn("_gold_built_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    persisted = spark.table(table_name)
    row_count = validate_model(persisted, table_name, key_columns)

    results.append((model_name, table_name, row_count))
    print(f"Validated {table_name}: {row_count:,} rows")

# COMMAND ----------

# Final persisted-table verification.
for model_name, table_name, expected_count in results:
    persisted_count = spark.table(table_name).count()
    if persisted_count != expected_count:
        raise AssertionError(
            f"{table_name}: expected {expected_count}, "
            f"found {persisted_count}"
        )

    required_metadata = {"_gold_run_id", "_gold_built_at"}
    actual_columns = set(spark.table(table_name).columns)
    missing = required_metadata - actual_columns
    if missing:
        raise AssertionError(
            f"{table_name}: missing Gold metadata {sorted(missing)}"
        )

print("\nGOLD ANALYTICS RESULTS")
for model_name, table_name, row_count in results:
    print(f"{model_name:32} {row_count:>10,} rows")

print(
    "\nPASS: all six Gold Delta tables were persisted, "
    "and their declared grain keys were validated."
)
