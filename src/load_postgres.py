"""Append new CSV records to PostgreSQL and checkpoint the committed prefix."""

import csv
import hashlib
import io
import logging
from decimal import Decimal, InvalidOperation

import psycopg
from psycopg.types.json import Jsonb

from src.monitoring import log_run, observe_run, record_run
from src.pipeline import BASE, RETAIL_COLUMNS, transform, validate_source_columns

MAX_WRITE_BATCH = 1000


def classify(record_number, row):
    try:
        transform(row)
        order_id = row["order_id"].strip()
        if not order_id:
            raise ValueError("Missing order_id")
        if any("\0" in key or "\0" in value for key, value in row.items()):
            raise ValueError("NUL byte in CSV record")
        # Keep the source price as a decimal for dbt's revenue calculations.
        price = Decimal(row["unit_price"])
        if price and price.as_tuple().exponent < -16383:
            raise ValueError("unit_price is below PostgreSQL numeric precision")
        if price < 0:
            raise ValueError("negative unit_price")
        return True, (
            record_number, order_id, row["product_id"].strip(),
            int(row["quantity"]), price if price else Decimal(0), int(row["stock"]),
        )
    except (ValueError, InvalidOperation) as error:
        # JSONB cannot store NUL even when escaped. Preserve other payload text.
        payload = {}
        for key, value in row.items():
            if key is None:
                payload["null"] = [item.replace("\0", "\ufffd") for item in value]
            else:
                payload[key.replace("\0", "\ufffd")] = (
                    value.replace("\0", "\ufffd") if value is not None else None
                )
        return False, (record_number, payload, str(error))


def read_records(path, checkpoint, state):
    with path.open("rb") as handle:
        offset, position, checksum = checkpoint
        digest = hashlib.sha256()
        remaining = offset
        while remaining:
            block = handle.read(min(remaining, 1024 * 1024))
            if not block:
                raise ValueError("CSV is shorter than the committed prefix")
            digest.update(block)
            remaining -= len(block)
        if checksum is not None and checksum != digest.hexdigest():
            raise ValueError("CSV committed prefix has changed")

        with io.TextIOWrapper(handle, encoding="utf-8", newline="") as source:
            source.seek(0)
            columns = validate_source_columns(
                next(csv.reader(source, strict=True), []), RETAIL_COLUMNS | {"order_id"},
            )
            source.seek(offset)
            last_line = ""

            def tracked_lines():
                nonlocal offset, last_line
                for line in source:
                    data = line.encode("utf-8")
                    offset += len(data)
                    digest.update(data)
                    last_line = line
                    yield line

            lines = tracked_lines()
            reader = csv.DictReader(lines, fieldnames=columns, strict=True)
            if checksum is None:
                next(csv.reader(lines, strict=True))
            for position, row in enumerate(reader, start=position + 1):
                yield position, row

            if last_line and not last_line.endswith(("\n", "\r")):
                raise ValueError("CSV must end with a record terminator before loading")
            state[:] = [offset, position, digest.hexdigest()]


def legacy_rows(cursor):
    cursor.execute("SELECT source_row, order_id, product_id, quantity, unit_price, stock FROM raw.orders")
    rows = {row[0]: (True, row) for row in cursor}
    cursor.execute("SELECT source_row, payload, reason FROM raw.rejected_orders")
    for row in cursor:
        if row[0] in rows:
            raise ValueError("Legacy CSV tables contain overlapping source rows")
        rows[row[0]] = (False, row)
    return rows


def write_rows(cursor, accepted, rejected):
    cursor.executemany(
        "INSERT INTO raw.orders (source_row, order_id, product_id, quantity, unit_price, stock)"
        " VALUES (%s, %s, %s, %s, %s, %s)", accepted,
    )
    cursor.executemany(
        "INSERT INTO raw.rejected_orders (source_row, payload, reason) VALUES (%s, %s, %s)",
        ((number, Jsonb(payload), reason) for number, payload, reason in rejected),
    )
    accepted.clear()
    rejected.clear()


def load_orders(path):
    accepted = []
    rejected = []
    valid_count = rejected_count = 0
    state = []

    with (
        observe_run("csv") as (started, threshold),
        psycopg.connect() as connection,
        connection.cursor() as cursor,
    ):
        # Serialize checkpoint readers and writers, including the first load.
        cursor.execute("LOCK TABLE raw.csv_checkpoint IN SHARE ROW EXCLUSIVE MODE")
        cursor.execute("SELECT byte_offset, source_row, prefix_sha256 FROM raw.csv_checkpoint")
        checkpoint = cursor.fetchone()
        legacy = legacy_rows(cursor) if checkpoint is None else {}
        legacy_end = max(legacy, default=0)
        for record_number, row in read_records(path, checkpoint or (0, 0, None), state):
            valid, record = classify(record_number, row)
            if record_number <= legacy_end:
                if legacy.pop(record_number, None) != (valid, record):
                    raise ValueError("CSV does not match the existing warehouse history")
                continue
            if valid:
                accepted.append(record)
                valid_count += 1
            else:
                rejected.append(record)
                rejected_count += 1
            if len(accepted) + len(rejected) == MAX_WRITE_BATCH:
                write_rows(cursor, accepted, rejected)

        if legacy:
            raise ValueError("CSV is missing existing warehouse records")
        write_rows(cursor, accepted, rejected)
        cursor.execute(
            """
            INSERT INTO raw.csv_checkpoint (byte_offset, source_row, prefix_sha256)
            VALUES (%s, %s, %s)
            ON CONFLICT (singleton) DO UPDATE SET
                byte_offset = EXCLUDED.byte_offset, source_row = EXCLUDED.source_row,
                prefix_sha256 = EXCLUDED.prefix_sha256
            """,
            state,
        )
        seen = valid_count + rejected_count
        metrics = record_run(cursor, "csv", started, seen, rejected_count, seen, threshold)

    log_run(metrics)
    return valid_count, rejected_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_orders(BASE / "data" / "input.csv")
