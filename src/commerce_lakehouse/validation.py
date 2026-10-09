"""Reusable structural and business-rule validation for CommerceLake."""

import csv
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from commerce_lakehouse.contracts import CONTRACTS

Row = Mapping[str, str]
DatasetRows = Mapping[str, Sequence[Row]]


def validate_headers(headers: Mapping[str, Sequence[str]]) -> list[str]:
    """Validate dataset presence and exact source CSV column names."""
    errors: list[str] = []

    for name, contract in CONTRACTS.items():
        if name not in headers:
            errors.append(f"{name}: missing dataset header")
            continue

        actual = list(headers[name])
        expected = list(contract.columns)

        if len(actual) != len(set(actual)):
            errors.append(f"{name}: duplicate column names")

        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))

        if missing:
            errors.append(f"{name}: missing columns: {', '.join(missing)}")
        if unexpected:
            errors.append(f"{name}: unexpected columns: {', '.join(unexpected)}")

    for name in sorted(set(headers) - set(CONTRACTS)):
        errors.append(f"{name}: unexpected dataset")

    return errors


def _decimal(value: str) -> Decimal:
    """Parse a finite DECIMAL(12,2) value."""
    number = Decimal(value)

    if not number.is_finite():
        raise ValueError("value must be finite")

    exponent = number.as_tuple().exponent
    fractional_digits = max(0, -exponent)
    integer_digits = max(0, len(number.as_tuple().digits) + exponent)

    if fractional_digits > 2:
        raise ValueError("value has more than two decimal places")

    if integer_digits > 10:
        raise ValueError("value exceeds DECIMAL(12,2) precision")

    return number


def _timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp that includes a timezone offset."""
    parsed = datetime.fromisoformat(value)

    if parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")

    return parsed


def _check_primary_key(
    rows: Sequence[Row], dataset: str, field: str, errors: list[str]
) -> None:
    seen: set[str] = set()

    for index, row in enumerate(rows, start=2):
        value = row.get(field, "").strip()

        if not value:
            errors.append(f"{dataset} CSV line {index}: {field} is empty")
        elif value in seen:
            errors.append(
                f"{dataset} CSV line {index}: duplicate {field}={value}"
            )
        else:
            seen.add(value)


def validate_datasets(
    headers: Mapping[str, Sequence[str]],
    datasets: DatasetRows,
) -> list[str]:
    """Return all detected contract and business-rule violations.

    Input values are CSV strings. The function does not mutate input data.
    It deliberately reports errors rather than stopping at the first invalid row.
    """
    errors = validate_headers(headers)

    for name in CONTRACTS:
        if name not in datasets:
            errors.append(f"{name}: missing dataset rows")

    # A schema-invalid dataset cannot be safely interpreted using the contract.
    usable: dict[str, Sequence[Row]] = {}
    for name, contract in CONTRACTS.items():
        if name not in datasets or name not in headers:
            continue

        if set(headers[name]) != set(contract.columns):
            continue

        if len(headers[name]) != len(set(headers[name])):
            continue

        normalized_rows: list[dict[str, str]] = []
        for index, row in enumerate(datasets[name], start=2):
            location = f"{name} CSV line {index}"

            # DictReader stores surplus CSV fields under the None key.
            extras = row.get(None)
            if isinstance(extras, list) and extras:
                errors.append(
                    f"{location}: {len(extras)} unexpected extra CSV field(s)"
                )

            unexpected_keys = set(row) - set(contract.columns) - {None}
            if unexpected_keys:
                errors.append(
                    f"{location}: unexpected row fields: "
                    f"{', '.join(sorted(str(key) for key in unexpected_keys))}"
                )

            normalized: dict[str, str] = {}
            missing_cells: list[str] = []
            for field in contract.columns:
                value = row.get(field, "")
                if value is None or field not in row:
                    missing_cells.append(field)
                    normalized[field] = ""
                elif not isinstance(value, str):
                    errors.append(
                        f"{location}: {field} must be a string value"
                    )
                    normalized[field] = ""
                else:
                    normalized[field] = value

            if missing_cells:
                errors.append(
                    f"{location}: missing CSV value(s) for column(s): "
                    f"{', '.join(missing_cells)}"
                )

            normalized_rows.append(normalized)

        usable[name] = normalized_rows

    # Validate record-level fields and primitive types.
    for name, contract in CONTRACTS.items():
        rows = usable.get(name)
        if rows is None:
            continue

        _check_primary_key(rows, name, contract.primary_key, errors)

        for index, row in enumerate(rows, start=2):
            location = f"{name} CSV line {index}"

            for field in contract.required_fields:
                value = row.get(field, "")
                if not value.strip():
                    errors.append(f"{location}: {field} is empty")

            for field, allowed in (contract.allowed_values or {}).items():
                value = row.get(field, "").strip()
                if value and value not in allowed:
                    errors.append(f"{location}: invalid {field}={value!r}")

            for field, data_type in contract.columns.items():
                value = row.get(field, "").strip()
                if not value:
                    continue

                try:
                    if data_type == "TIMESTAMP":
                        _timestamp(value)
                    elif data_type == "INT":
                        parsed_int = int(value)
                        if str(parsed_int) != value and value not in {
                            f"+{parsed_int}", f"-{abs(parsed_int)}"
                        }:
                            raise ValueError("value must be an integer")
                        if field in {"quantity", "quantity_returned"} and parsed_int <= 0:
                            errors.append(f"{location}: {field} must be positive")
                    elif data_type.startswith("DECIMAL"):
                        parsed_decimal = _decimal(value)
                        if parsed_decimal < 0:
                            errors.append(f"{location}: {field} must not be negative")
                    elif data_type == "BOOLEAN" and value not in {"true", "false"}:
                        errors.append(
                            f"{location}: {field} must be true or false"
                        )
                except (ValueError, InvalidOperation, TypeError):
                    errors.append(
                        f"{location}: invalid {data_type} value for {field}: "
                        f"{value!r}"
                    )

    # Build parent indexes only from rows with nonempty keys.
    indexes: dict[str, dict[str, Row]] = {}
    for name, contract in CONTRACTS.items():
        rows = usable.get(name, [])
        indexes[name] = {
            row[contract.primary_key]: row
            for row in rows
            if row.get(contract.primary_key, "").strip()
        }

    # Referential integrity.
    for name, contract in CONTRACTS.items():
        for relationship in contract.foreign_keys:
            parent = indexes.get(relationship.parent_dataset, {})
            for index, row in enumerate(usable.get(name, []), start=2):
                value = row.get(relationship.field, "").strip()
                if value and value not in parent:
                    errors.append(
                        f"{name} CSV line {index}: {relationship.field} "
                        f"references unknown "
                        f"{relationship.parent_dataset} ID {value!r}"
                    )

    customers = indexes.get("customers", {})
    orders = indexes.get("orders", {})
    items = indexes.get("order_items", {})

    # Orders cannot predate customer signup.
    for order in usable.get("orders", []):
        customer = customers.get(order.get("customer_id", ""))
        if not customer:
            continue
        try:
            if _timestamp(order["order_timestamp"]) < _timestamp(
                customer["signup_timestamp"]
            ):
                errors.append(
                    f"orders: {order['order_id']} predates customer signup"
                )
        except (ValueError, TypeError, KeyError):
            pass  # The malformed timestamp has already been reported.

    # Item gross by order, using decimal arithmetic.
    item_gross: dict[str, Decimal] = defaultdict(Decimal)
    for item in usable.get("order_items", []):
        try:
            item_gross[item["order_id"]] += (
                _decimal(item["unit_price"]) * int(item["quantity"])
            )
        except (ValueError, InvalidOperation, TypeError, KeyError):
            pass  # Primitive validation reports malformed values.

    # Payment currency and captured-payment reconciliation.
    captured_by_order: dict[str, list[Decimal]] = defaultdict(list)

    for payment in usable.get("payments", []):
        order_id = payment.get("order_id", "")
        order = orders.get(order_id)
        if order:
            payment_currency = payment.get("currency", "")
            if payment_currency and payment_currency != order.get("currency"):
                errors.append(
                    f"payments: {payment.get('payment_id')} currency "
                    f"{payment_currency} differs from order {order_id} "
                    f"currency {order.get('currency')}"
                )

        if payment.get("payment_status") == "captured":
            try:
                captured_by_order[order_id].append(_decimal(payment["amount"]))
            except (ValueError, InvalidOperation, TypeError, KeyError):
                pass

    for order_id, order in orders.items():
        captured = captured_by_order[order_id]

        if order.get("order_status") == "cancelled":
            if captured:
                errors.append(
                    f"orders: cancelled order {order_id} has captured payment"
                )
        elif order_id in item_gross:
            if len(captured) != 1:
                errors.append(
                    f"orders: {order_id} expected one captured payment, "
                    f"found {len(captured)}"
                )
            elif captured[0] != item_gross[order_id]:
                errors.append(
                    f"orders: {order_id} captured payment {captured[0]} "
                    f"does not equal item gross {item_gross[order_id]}"
                )

    # Returns must follow delivery and cannot exceed purchased quantities.
    returned_by_item: dict[str, int] = defaultdict(int)

    for returned in usable.get("returns", []):
        item_id = returned.get("order_item_id", "")
        item = items.get(item_id)
        if not item:
            continue

        order = orders.get(item.get("order_id", ""))
        if not order:
            continue

        try:
            return_time = _timestamp(returned["return_timestamp"])
            order_time = _timestamp(order["order_timestamp"])

            if order.get("order_status") != "delivered":
                errors.append(
                    f"returns: {returned.get('return_id')} belongs to an "
                    f"order that is not delivered"
                )

            if return_time < order_time:
                errors.append(
                    f"returns: {returned.get('return_id')} predates its order"
                )

            returned_by_item[item_id] += int(returned["quantity_returned"])
        except (ValueError, TypeError, KeyError):
            pass

    for item_id, returned_quantity in returned_by_item.items():
        try:
            purchased_quantity = int(items[item_id]["quantity"])
            if returned_quantity > purchased_quantity:
                errors.append(
                    f"returns: {item_id} returned quantity {returned_quantity} "
                    f"exceeds purchased quantity {purchased_quantity}"
                )
        except (ValueError, TypeError, KeyError):
            pass

    return errors


def load_csv_datasets(
    data_directory: Path,
) -> tuple[dict[str, list[str]], dict[str, list[dict[str, str]]]]:
    """Load the six contracted CSV files and retain their headers."""
    headers: dict[str, list[str]] = {}
    datasets: dict[str, list[dict[str, str]]] = {}

    for name in CONTRACTS:
        path = data_directory / f"{name}.csv"
        if not path.is_file():
            continue

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            headers[name] = reader.fieldnames or []
            datasets[name] = list(reader)

    return headers, datasets
