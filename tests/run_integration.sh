#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
project="retail-ci-$$"
compose() {
  DQ_ERROR_RATE_THRESHOLD=0 docker compose --project-name "$project" --profile streaming "$@"
}
cleanup() {
  if [[ $1 != 0 ]]; then
    compose logs --no-color --tail 150 warehouse kafka kafka-init spark || true
  fi
  compose down --volumes --remove-orphans
}
trap 'cleanup "$?"' EXIT

batch_python() {
  compose run --rm --no-deps -T --workdir /opt/retail \
    --volume "$PWD/tests:/opt/retail/tests:ro" \
    --env EXPECTED_STREAM_EVENTS \
    --entrypoint /home/airflow/retail-venv/bin/python airflow "$@"
}

publish() {
  local event
  event='{"event_id":"EVENT-ID","event_time":"2025-01-01T12:00:00Z","product_id":"P1","quantity":2,"unit_price":15.50}'
  printf '%s\n' "${event/EVENT-ID/$1}"
}

compose build airflow spark
compose up -d --wait warehouse
batch_python -m unittest discover -s tests/integration -p test_batch.py -v
compose run --rm --no-deps -T --volume "$PWD/tests:/opt/retail/tests:ro" --entrypoint python spark \
  -m unittest discover -s tests/integration -p test_spark.py -v
compose up -d spark
{
  publish E1
  publish E1
  printf '%s\n' 'invalid-test-json'
} | compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic retail-orders
EXPECTED_STREAM_EVENTS=1 batch_python -m unittest discover -s tests/integration -p test_stream.py -v
batch_id=$(compose exec -T warehouse psql -U retail -d retail -XAt -c \
  "SELECT max(batch_id) FROM raw.pipeline_runs WHERE pipeline = 'kafka'")
for attempt in {1..30}; do
  if compose exec -T spark test -f "/checkpoints/retail-orders/commits/$batch_id"; then
    break
  fi
  sleep 2
done
compose exec -T spark test -f "/checkpoints/retail-orders/commits/$batch_id"
compose restart spark
publish E2 | compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic retail-orders
EXPECTED_STREAM_EVENTS=2 batch_python -m unittest discover -s tests/integration -p test_stream.py -v
