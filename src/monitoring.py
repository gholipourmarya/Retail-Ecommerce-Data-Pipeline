"""Persist per-run metrics and emit JSON log messages."""

import json
import logging
import os
from contextlib import contextmanager
from time import perf_counter

LOGGER = logging.getLogger(__name__)


def quality_threshold():
    threshold = float(os.environ.get("DQ_ERROR_RATE_THRESHOLD", "0"))
    if not 0 <= threshold <= 1:
        raise ValueError("DQ_ERROR_RATE_THRESHOLD must be between 0 and 1")
    return threshold


@contextmanager
def observe_run(pipeline, batch_id=None):
    started = perf_counter()
    try:
        yield started, quality_threshold()
    except Exception as error:
        LOGGER.error(json.dumps({
            "event": "pipeline_run_failed",
            "pipeline": pipeline,
            "batch_id": batch_id,
            "duration_seconds": perf_counter() - started,
            "error_type": type(error).__name__,
        }))
        raise


def record_run(cursor, pipeline, started, seen, rejected, written, threshold, batch_id=None):
    duration = perf_counter() - started
    cursor.execute(
        """
        INSERT INTO raw.pipeline_runs (
            pipeline, batch_id, rows_seen, rows_rejected, rows_written,
            duration_seconds, alert_threshold
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING run_id
        """,
        (pipeline, batch_id, seen, rejected, written, duration, threshold),
    )
    run_id, = cursor.fetchone()
    error_rate = rejected / seen if seen else 0
    return {
        "run_id": run_id,
        "pipeline": pipeline,
        "batch_id": batch_id,
        "rows_seen": seen,
        "rows_valid": seen - rejected,
        "rows_rejected": rejected,
        "rows_written": written,
        "rows_skipped": seen - written,
        "error_rate": error_rate,
        "duration_seconds": duration,
        "alert_threshold": threshold,
        "alert": error_rate > threshold,
    }


def log_run(metrics):
    level = logging.WARNING if metrics["alert"] else logging.INFO
    LOGGER.log(level, json.dumps({"event": "pipeline_run", **metrics}))
