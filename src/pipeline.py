"""Small dependency-free ETL example. Input is synthetic demonstration data."""
import csv
import json
import sqlite3
from contextlib import closing
from math import isfinite
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DOMAIN = "retail"
RETAIL_COLUMNS = {'product_id', 'quantity', 'unit_price', 'stock'}

def transform(row):
    """Return normalized record or raise ValueError for invalid source rows."""
    out = dict(row)
    try:
        if None in row or None in row.values():
            raise ValueError('Wrong number of CSV fields')
        if DOMAIN == 'retail':
            if not row['product_id'] or not row['product_id'].strip():
                raise ValueError('missing product_id')
            qty = int(row['quantity'])
            price = float(row['unit_price'])
            stock = int(row['stock'])
            if not isfinite(price):
                raise ValueError('unit_price must be finite')
            if min(qty, price, stock) < 0:
                raise ValueError('negative retail value')
            value = qty * price
            if not isfinite(value):
                raise ValueError('row value is too large')
            out['value'] = round(value, 2)
            out['flag'] = int(stock < 15)
        elif DOMAIN == 'iot':
            temp, vibration, pressure = float(row['temperature_c']), float(row['vibration_mm_s']), float(row['pressure_bar'])
            if pressure < 0: raise ValueError('negative pressure')
            out['value'] = 1.0
            out['flag'] = int(temp > 85 or vibration > 6)
        elif DOMAIN == 'banking':
            amount, hour = float(row['amount']), int(row['hour'])
            if amount < 0 or not 0 <= hour <= 23: raise ValueError('invalid transaction')
            out['value'] = amount
            out['flag'] = int(amount >= 10000 or hour < 5)
        elif DOMAIN == 'energy':
            consumption = float(row['consumption_kwh'])
            if consumption < 0: raise ValueError('negative consumption')
            out['value'] = consumption
            out['flag'] = int(consumption > 30)
        elif DOMAIN == 'healthcare':
            tests = int(row['lab_tests'])
            if tests < 0 or row['status'] not in ('completed', 'cancelled', 'scheduled'): raise ValueError('invalid appointment')
            out['value'] = 1.0
            out['flag'] = int(row['status'] == 'cancelled')
        else: raise ValueError('unknown domain')
    except (KeyError, OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f'Invalid row: {exc}') from exc
    return out


def validate_source_columns(columns, required_columns):
    missing = required_columns.difference(columns)
    if missing:
        raise ValueError(f'Missing CSV columns: {", ".join(sorted(missing))}')
    if len(set(columns)) != len(columns):
        raise ValueError('Duplicate CSV column names')
    return columns


def read_source_rows(path, required_columns=RETAIL_COLUMNS):
    with path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle, strict=True)
        validate_source_columns(reader.fieldnames or [], required_columns)
        yield from reader


def run():
    input_path = BASE / 'data' / 'input.csv'
    output = BASE / 'output'
    valid, rejected = [], []
    for row in read_source_rows(input_path):
        try:
            valid.append(transform(row))
        except ValueError as exc:
            rejected.append({'row': row, 'reason': str(exc)})

    output.mkdir(exist_ok=True)
    with closing(sqlite3.connect(output / 'warehouse.db')) as con, con:
        con.execute('BEGIN')
        con.execute('DROP TABLE IF EXISTS events')
        con.execute('CREATE TABLE events (group_key TEXT NOT NULL, value REAL NOT NULL, flag INTEGER NOT NULL)')
        con.executemany(
            'INSERT INTO events VALUES (?, ?, ?)',
            ((row['product_id'], row['value'], row['flag']) for row in valid),
        )
        result = con.execute(
            'SELECT group_key, COUNT(*), ROUND(SUM(value), 2), SUM(flag) '
            'FROM events GROUP BY group_key ORDER BY group_key'
        ).fetchall()
        summary = []
        for group, records, value, flags in result:
            if not isfinite(value):
                raise ValueError(f'Total value is too large for product {group!r}')
            summary.append({'group': group, 'records': records, 'total_value': value, 'flags': flags})

    with (output / 'clean.jsonl').open('w', encoding='utf-8') as handle:
        for row in valid:
            handle.write(json.dumps(row) + '\n')
    (output / 'rejected.json').write_text(json.dumps(rejected, indent=2), encoding='utf-8')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({'accepted': len(valid), 'rejected': len(rejected), 'summary': summary}, indent=2))
    return summary

if __name__ == '__main__': run()
