import json
import os
from pathlib import Path
import signal
import stat
import sys
import tempfile
import textwrap
import unittest

from scripts import run_historical_performance_canary as canary


class HistoricalPerformanceCanaryTest(unittest.TestCase):
    def test_parses_only_progress_slot_lines(self) -> None:
        self.assertEqual(
            canary.parse_progress_slot("progress slot 87304001/87695515 (9.2%)"),
            87_304_001,
        )
        self.assertIsNone(canary.parse_progress_slot("block slot 87304001"))

    def test_receipt_is_owner_only_durable_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            canary.write_json_noclobber(path, {"value": 7})
            self.assertEqual(json.loads(path.read_text()), {"value": 7})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                canary.write_json_noclobber(path, {"value": 8})

    def test_stops_child_at_target_and_records_actual_overshoot(self) -> None:
        child = textwrap.dedent(
            """
            import signal
            import sys
            import time

            signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
            print("progress slot 99/200", flush=True)
            print("progress slot 101/200", flush=True)
            while True:
                time.sleep(0.05)
            """
        )
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "receipt.json"
            result = canary.run_canary(100, receipt, [sys.executable, "-c", child])
            self.assertEqual(result, 0)
            payload = json.loads(receipt.read_text())
            self.assertTrue(payload["target_reached"])
            self.assertEqual(payload["observed_stop_slot"], 101)
            self.assertEqual(payload["observed_overshoot_slots"], 1)
            self.assertEqual(payload["child_return_code"], 0)
            self.assertIsNone(payload["external_signal"])

    def test_refuses_non_owner_only_receipt_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o755)
            with self.assertRaisesRegex(ValueError, "not owner-only"):
                canary.write_json_noclobber(Path(directory) / "receipt.json", {})


if __name__ == "__main__":
    unittest.main()
