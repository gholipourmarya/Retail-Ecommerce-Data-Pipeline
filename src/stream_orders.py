"""Read order events from Kafka and write validated micro-batches to PostgreSQL."""

import logging

import psycopg
from pyspark.sql import SparkSession, functions as F, types as T

from src.monitoring import log_run, observe_run, record_run


EVENT = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DecimalType(18, 2)),
    T.StructField("_corrupt_record", T.StringType()),
])


def parse_orders(messages):
    orders = messages.select(
        "topic", "partition", "offset", F.col("value").cast("string").alias("payload"),
    ).withColumn("event", F.from_json("payload", EVENT, {"columnNameOfCorruptRecord": "_corrupt_record"}))
    event_id = F.trim(F.col("event.event_id"))
    product_id = F.trim(F.col("event.product_id"))
    event_time = F.col("event.event_time")
    quantity = F.col("event.quantity")
    unit_price = F.col("event.unit_price")
    valid = (
        F.col("event._corrupt_record").isNull()
        & (F.length(event_id) > 0)
        & (F.length(product_id) > 0)
        & ~event_id.contains("\0")
        & ~product_id.contains("\0")
        & event_time.isNotNull()
        & (quantity >= 0)
        & (unit_price >= 0)
    )

    return orders.select(
        "topic", "partition", "offset", "payload",
        F.when(valid, event_id).alias("event_id"),
        F.when(valid, event_time).alias("event_time"),
        F.when(valid, product_id).alias("product_id"),
        F.when(valid, quantity * unit_price).alias("revenue"),
    )


def save_batch(batch, batch_id):
    seen = rejected = 0

    def rows():
        nonlocal seen, rejected
        for row in batch.toLocalIterator():
            seen += 1
            rejected += row.event_id is None
            # PostgreSQL text cannot store NUL bytes. Kafka keeps the original payload.
            payload = row.payload.replace("\0", "\ufffd") if row.payload is not None else None
            yield (
                row.topic, row.partition, row.offset, payload,
                row.event_id, row.event_time, row.product_id, row.revenue,
            )

    with (
        observe_run("kafka", batch_id) as (started, threshold),
        psycopg.connect() as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        cursor.executemany(
            """
            INSERT INTO raw.stream_orders (
                topic, kafka_partition, kafka_offset, payload,
                event_id, event_time, product_id, revenue
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows(),
        )
        if not seen:
            return
        metrics = record_run(cursor, "kafka", started, seen, rejected, cursor.rowcount, threshold, batch_id)

    log_run(metrics)


def main():
    logging.basicConfig(level=logging.INFO)
    spark = (
        SparkSession.builder.appName("retail-orders")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "retail-orders")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )
    query = (
        parse_orders(messages).writeStream.foreachBatch(save_batch)
        .option("checkpointLocation", "/checkpoints/retail-orders")
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
