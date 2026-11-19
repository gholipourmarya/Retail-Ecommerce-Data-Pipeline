import csv
import logging
import os
import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import psycopg

from src.load_postgres import load_orders

ROOT = Path(__file__).resolve().parents[2]
HEADER = "order_id,product_id,quantity,unit_price,stock\n"


def query(statement):
    with psycopg.connect() as connection:
        return connection.execute(statement).fetchall()


class BatchIntegrationTests(unittest.TestCase):
    def build_dbt(self, workdir):
        project = str(ROOT / "dbt")
        subprocess.run(
            [
                str(Path(sys.executable).with_name("dbt")), "--no-use-colors", "build",
                "--project-dir", project, "--profiles-dir", project,
                "--target-path", str(workdir / "target"), "--log-path", str(workdir / "logs"),
            ],
            check=True,
            env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
        )

    def test_legacy_checkpoint_append_retry_and_incremental_mart(self):
        query("""
            INSERT INTO raw.orders VALUES
                (1, 'O1', 'P1', 2, 15.50, 40), (2, 'O2', 'P2', 1, 80, 12)
            RETURNING source_row
        """)
        with TemporaryDirectory() as directory:
            workdir = Path(directory)
            csv_path = workdir / "orders.csv"
            original = HEADER + "O1,P1,2,15.50,40\nO2,P2,1,80,12\n"
            csv_path.write_text(original, encoding="utf-8")
            self.assertEqual(load_orders(csv_path), (0, 0))
            self.build_dbt(workdir)
            untouched, = query("SELECT xmin::text FROM reporting.product_sales WHERE product_id = 'P2'")

            appended = original + 'O3,P1,3,15.50,5\n"O\n4",P3,1,2.50,20\nO5,P1,-1,2,9\n'
            csv_path.write_text(appended, encoding="utf-8")
            with self.assertLogs("src.monitoring", level=logging.WARNING) as logs:
                self.assertEqual(load_orders(csv_path), (2, 1))
            self.assertIn('"alert": true', logs.output[0])
            self.build_dbt(workdir)
            self.assertEqual(query("SELECT xmin::text FROM reporting.product_sales WHERE product_id = 'P2'"),
                             [untouched])
            self.assertEqual(query("""
                SELECT product_id, order_lines, revenue, low_stock_lines, last_source_row
                FROM reporting.product_sales ORDER BY product_id
            """), [
                ("P1", 2, Decimal("77.50"), 1, 3),
                ("P2", 1, Decimal("80.00"), 1, 2),
                ("P3", 1, Decimal("2.50"), 0, 4),
            ])
            self.assertEqual(load_orders(csv_path), (0, 0))
            self.build_dbt(workdir)

            checkpoint = query("SELECT byte_offset, source_row, prefix_sha256 FROM raw.csv_checkpoint")
            csv_path.write_text(appended + 'O6,P3,1,1,2\n"unclosed', encoding="utf-8")
            with patch("src.load_postgres.MAX_WRITE_BATCH", 1):
                with self.assertLogs("src.monitoring", level="ERROR") as logs:
                    with self.assertRaisesRegex(csv.Error, "unexpected end of data"):
                        load_orders(csv_path)
            self.assertIn('"event": "pipeline_run_failed"', logs.output[0])
            csv_path.write_text(appended.replace("15.50", "15.51"), encoding="utf-8")
            with self.assertLogs("src.monitoring", level="ERROR"):
                with self.assertRaisesRegex(ValueError, "committed prefix has changed"):
                    load_orders(csv_path)
            self.assertEqual(query("SELECT byte_offset, source_row, prefix_sha256 FROM raw.csv_checkpoint"),
                             checkpoint)
            self.assertEqual(query("SELECT source_row FROM raw.orders ORDER BY source_row"),
                             [(1,), (2,), (3,), (4,)])
            self.assertEqual(query("SELECT source_row FROM raw.rejected_orders"), [(5,)])
            self.assertEqual(query("""
                SELECT rows_seen, rows_rejected, rows_written, error_rate, alert
                FROM reporting.pipeline_quality WHERE pipeline = 'csv' ORDER BY run_id
            """), [(0, 0, 0, 0, False), (3, 1, 3, 1 / 3, True), (0, 0, 0, 0, False)])
