import os
import time
import unittest
from decimal import Decimal

import psycopg


class StreamIntegrationTests(unittest.TestCase):
    def test_kafka_to_spark_to_postgres(self):
        expected_events = int(os.environ["EXPECTED_STREAM_EVENTS"])
        expected_seen = expected_events + 2
        with psycopg.connect(autocommit=True) as connection:
            deadline = time.monotonic() + 300
            while True:
                metrics = connection.execute("""
                    SELECT sum(rows_seen), sum(rows_rejected), sum(rows_written),
                           bool_or(alert), bool_and(duration_seconds >= 0)
                    FROM reporting.pipeline_quality WHERE pipeline = 'kafka'
                """).fetchone()
                if metrics[0] == expected_seen or time.monotonic() >= deadline:
                    break
                time.sleep(2)

            self.assertEqual(metrics, (expected_seen, 1, expected_events + 1, True, True))
            self.assertEqual(connection.execute("""
                SELECT events, revenue FROM reporting.stream_product_sales WHERE product_id = 'P1'
            """).fetchall(), [(expected_events, Decimal("31.00") * expected_events)])
            self.assertEqual(connection.execute("""
                SELECT payload FROM raw.stream_orders WHERE event_id IS NULL
            """).fetchall(), [("invalid-test-json",)])
