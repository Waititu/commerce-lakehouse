"""Canonical source data contracts for CommerceLake.

These contracts describe source CSV schemas and relational constraints.
Business-rule execution is implemented separately in validation.py.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ForeignKey:
    """A relationship from a child dataset to a parent dataset."""

    field: str
    parent_dataset: str
    parent_field: str


@dataclass(frozen=True)
class DatasetContract:
    """Structural contract for one source dataset."""

    name: str
    columns: dict[str, str]
    primary_key: str
    required_fields: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()
    allowed_values: dict[str, frozenset[str]] | None = None


COUNTRIES = frozenset({"KE", "US", "GB", "CA", "DE"})
CURRENCIES = frozenset({"USD", "KES", "GBP", "EUR", "CAD"})
CUSTOMER_SEGMENTS = frozenset({"standard", "premium", "business"})
ORDER_STATUSES = frozenset(
    {"pending", "confirmed", "shipped", "delivered", "cancelled"}
)
PAYMENT_STATUSES = frozenset(
    {"initiated", "authorized", "captured", "failed", "refunded"}
)

CONTRACTS: dict[str, DatasetContract] = {
    "customers": DatasetContract(
        name="customers",
        columns={
            "customer_id": "STRING",
            "signup_timestamp": "TIMESTAMP",
            "country_code": "STRING",
            "customer_segment": "STRING",
        },
        primary_key="customer_id",
        required_fields=(
            "customer_id",
            "signup_timestamp",
            "country_code",
            "customer_segment",
        ),
        allowed_values={
            "country_code": COUNTRIES,
            "customer_segment": CUSTOMER_SEGMENTS,
        },
    ),
    "products": DatasetContract(
        name="products",
        columns={
            "product_id": "STRING",
            "product_name": "STRING",
            "category": "STRING",
            "unit_price": "DECIMAL(12,2)",
            "currency": "STRING",
            "is_active": "BOOLEAN",
        },
        primary_key="product_id",
        required_fields=(
            "product_id",
            "product_name",
            "category",
            "unit_price",
            "currency",
            "is_active",
        ),
        allowed_values={"currency": CURRENCIES},
    ),
    "orders": DatasetContract(
        name="orders",
        columns={
            "order_id": "STRING",
            "customer_id": "STRING",
            "order_timestamp": "TIMESTAMP",
            "order_status": "STRING",
            "currency": "STRING",
        },
        primary_key="order_id",
        required_fields=(
            "order_id",
            "customer_id",
            "order_timestamp",
            "order_status",
            "currency",
        ),
        foreign_keys=(
            ForeignKey("customer_id", "customers", "customer_id"),
        ),
        allowed_values={
            "order_status": ORDER_STATUSES,
            "currency": CURRENCIES,
        },
    ),
    "order_items": DatasetContract(
        name="order_items",
        columns={
            "order_item_id": "STRING",
            "order_id": "STRING",
            "product_id": "STRING",
            "quantity": "INT",
            "unit_price": "DECIMAL(12,2)",
        },
        primary_key="order_item_id",
        required_fields=(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ),
        foreign_keys=(
            ForeignKey("order_id", "orders", "order_id"),
            ForeignKey("product_id", "products", "product_id"),
        ),
    ),
    "payments": DatasetContract(
        name="payments",
        columns={
            "payment_id": "STRING",
            "order_id": "STRING",
            "payment_timestamp": "TIMESTAMP",
            "payment_status": "STRING",
            "amount": "DECIMAL(12,2)",
            "currency": "STRING",
            "payment_method": "STRING",
        },
        primary_key="payment_id",
        required_fields=(
            "payment_id",
            "order_id",
            "payment_timestamp",
            "payment_status",
            "amount",
            "currency",
            "payment_method",
        ),
        foreign_keys=(
            ForeignKey("order_id", "orders", "order_id"),
        ),
        allowed_values={
            "payment_status": PAYMENT_STATUSES,
            "currency": CURRENCIES,
        },
    ),
    "returns": DatasetContract(
        name="returns",
        columns={
            "return_id": "STRING",
            "order_item_id": "STRING",
            "return_timestamp": "TIMESTAMP",
            "quantity_returned": "INT",
            "refund_amount": "DECIMAL(12,2)",
            "return_reason": "STRING",
        },
        primary_key="return_id",
        required_fields=(
            "return_id",
            "order_item_id",
            "return_timestamp",
            "quantity_returned",
            "refund_amount",
            "return_reason",
        ),
        foreign_keys=(
            ForeignKey("order_item_id", "order_items", "order_item_id"),
        ),
    ),
}


def validate_contract_definitions() -> list[str]:
    """Detect inconsistencies in the contract definitions themselves."""
    errors: list[str] = []

    for name, contract in CONTRACTS.items():
        if name != contract.name:
            errors.append(f"{name}: registry key does not match contract name")

        if contract.primary_key not in contract.columns:
            errors.append(f"{name}: primary key is not a declared column")

        for field in contract.required_fields:
            if field not in contract.columns:
                errors.append(f"{name}: required field {field} is not declared")

        for field in contract.allowed_values or {}:
            if field not in contract.columns:
                errors.append(f"{name}: allowed-values field {field} is not declared")

        for relationship in contract.foreign_keys:
            if relationship.field not in contract.columns:
                errors.append(
                    f"{name}: foreign-key field {relationship.field} is not declared"
                )

            parent = CONTRACTS.get(relationship.parent_dataset)
            if parent is None:
                errors.append(
                    f"{name}: unknown parent dataset {relationship.parent_dataset}"
                )
            elif relationship.parent_field not in parent.columns:
                errors.append(
                    f"{name}: parent field {relationship.parent_dataset}."
                    f"{relationship.parent_field} is not declared"
                )

    return errors
