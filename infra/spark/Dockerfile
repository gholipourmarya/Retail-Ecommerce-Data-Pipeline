FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==4.0.0 psycopg[binary]==3.2.10

WORKDIR /opt/retail
ENV PYTHONPATH=/opt/retail
COPY src ./src
CMD ["spark-submit", "--master", "local[2]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "src/stream_orders.py"]
