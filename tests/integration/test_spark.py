import unittest
from decimal import Decimal

from pyspark.sql import SparkSession

from src.stream_orders import parse_orders


class SparkParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("retail-tests")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_valid_and_invalid_kafka_payloads(self):
        event = ('{"event_id":" E1 ","event_time":"2025-01-01T12:00:00Z",'
                 '"product_id":" P1 ","quantity":2,"unit_price":15.50}')
        payloads = [
            event,
            event.replace('" E1 "', '" "'),
            event.replace('" P1 "', r'"P\u00001"'),
            event.replace('"quantity":2', '"quantity":-2'),
            event.replace("2025-01-01T12:00:00Z", "bad date"),
            "invalid-json",
            None,
        ]
        messages = self.spark.createDataFrame(
            [("retail-orders", 0, offset, value) for offset, value in enumerate(payloads)],
            "topic string, partition int, offset long, value string",
        )
        rows = parse_orders(messages).orderBy("offset").collect()
        self.assertEqual((rows[0].event_id, rows[0].product_id, rows[0].revenue),
                         ("E1", "P1", Decimal("31.00")))
        for row in rows[1:]:
            self.assertIsNone(row.event_id)
            self.assertIsNone(row.revenue)
