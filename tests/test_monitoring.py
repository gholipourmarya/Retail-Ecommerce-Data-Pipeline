import json
import os
import unittest
from unittest.mock import Mock, patch

from src.monitoring import log_run, observe_run, quality_threshold, record_run


class QualityMonitoringTests(unittest.TestCase):
    def test_threshold_defaults_to_zero_and_rejects_invalid_settings(self):
        with patch.dict(os.environ):
            os.environ.pop("DQ_ERROR_RATE_THRESHOLD", None)
            self.assertEqual(quality_threshold(), 0)
        for threshold in ("-0.01", "1.01", "NaN", "inf", "invalid"):
            with self.subTest(threshold=threshold):
                with patch.dict(os.environ, {"DQ_ERROR_RATE_THRESHOLD": threshold}):
                    with self.assertRaises(ValueError):
                        quality_threshold()

    def test_metrics_alert_only_above_threshold(self):
        cursor = Mock()
        cursor.fetchone.return_value = (17,)
        cases = ((0, 0, 0, 0, False), (4, 1, 3, 0.25, False), (4, 1, 3, 0.2, True))
        for seen, rejected, written, threshold, alert in cases:
            with self.subTest(seen=seen, threshold=threshold):
                with patch("src.monitoring.perf_counter", return_value=1.5):
                    metrics = record_run(cursor, "kafka", 1, seen, rejected, written, threshold, 9)
                self.assertEqual(cursor.execute.call_args.args[1],
                                 ("kafka", 9, seen, rejected, written, 0.5, threshold))
                with self.assertLogs("src.monitoring", level="INFO") as logs:
                    log_run(metrics)
                self.assertEqual(logs.records[0].levelname, "WARNING" if alert else "INFO")
                payload = json.loads(logs.records[0].getMessage())
                self.assertEqual(payload["event"], "pipeline_run")
                self.assertEqual(payload["error_rate"], rejected / seen if seen else 0)
                self.assertEqual(payload["rows_skipped"], seen - written)
                self.assertEqual(payload["alert"], alert)

    def test_failures_are_logged_and_propagated(self):
        with patch.dict(os.environ, {"DQ_ERROR_RATE_THRESHOLD": "0.1"}):
            with self.assertLogs("src.monitoring", level="ERROR") as logs:
                with self.assertRaisesRegex(RuntimeError, "transaction failed"):
                    with observe_run("kafka", 9):
                        raise RuntimeError("transaction failed")
        payload = json.loads(logs.records[0].getMessage())
        self.assertEqual(payload["event"], "pipeline_run_failed")
        self.assertEqual(payload["pipeline"], "kafka")
        self.assertEqual(payload["batch_id"], 9)
        self.assertEqual(payload["error_type"], "RuntimeError")
        self.assertGreaterEqual(payload["duration_seconds"], 0)
