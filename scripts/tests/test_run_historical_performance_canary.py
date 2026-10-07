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
            canary.parse_progress_slot(
                "progress slot 87304001/87695515 (9.2%) txs=123 accounts=456"
            ),
            87_304_001,
        )
        self.assertEqual(
            canary.parse_progress(
                "prefix progress slot 87304001/87695515 (9.2%) txs=123 accounts=456 suffix"
            ),
            {"slot": 87_304_001, "transactions": 123, "account_updates": 456},
        )
        self.assertEqual(
            canary.parse_progress_slot("progress slot 87304001/87695515"),
            87_304_001,
        )
        self.assertIsNone(canary.parse_progress_slot("block slot 87304001"))

    def test_reads_cgroup_v2_counters_and_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            membership = root / "membership"
            cgroups = root / "cgroup"
            group = cgroups / "system.slice" / "test.service"
            group.mkdir(parents=True)
            membership.write_text("0::/system.slice/test.service\n")
            (group / "memory.current").write_text("100\n")
            (group / "memory.peak").write_text("200\n")
            (group / "pids.current").write_text("3\n")
            (group / "pids.peak").write_text("4\n")
            (group / "cpu.stat").write_text("usage_usec 40\nuser_usec 30\n")
            (group / "memory.stat").write_text("pgfault 12\npgmajfault 2\n")
            (group / "memory.events").write_text("high 5\noom 0\n")
            (group / "memory.pressure").write_text(
                "some avg10=0.00 avg60=0.00 avg300=0.00 total=7\n"
                "full avg10=0.00 avg60=0.00 avg300=0.00 total=3\n"
            )
            (group / "io.stat").write_text(
                "8:0 rbytes=10 wbytes=20 rios=1 wios=2\n"
                "8:1 rbytes=30 wbytes=40 rios=3 wios=4\n"
            )
            resolved = canary.current_cgroup_path(membership, cgroups)
            self.assertEqual(resolved, group.resolve())
            sample = canary.cgroup_snapshot(resolved)
            self.assertEqual(sample["gauges"]["memory.current"], 100)
            self.assertEqual(sample["counters"]["memory_stat.pgmajfault"], 2)
            self.assertEqual(sample["counters"]["memory_pressure.full_total_usec"], 3)
            self.assertEqual(sample["counters"]["io.rbytes"], 40)
            self.assertEqual(
                canary.counter_deltas(
                    {"same": 4, "reset": 8}, {"same": 9, "reset": 2}
                ),
                {"same": 5},
            )

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
            print("progress slot 99/200 txs=10 accounts=20", flush=True)
            print("progress slot 101/200 txs=14 accounts=30", flush=True)
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
            self.assertEqual(payload["first_progress"]["transactions"], 10)
            self.assertEqual(payload["final_progress"]["account_updates"], 30)
            self.assertGreaterEqual(payload["progress_rates"]["transactions_per_second"], 0)

    def test_refuses_non_owner_only_receipt_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o755)
            with self.assertRaisesRegex(ValueError, "not owner-only"):
                canary.write_json_noclobber(Path(directory) / "receipt.json", {})


if __name__ == "__main__":
    unittest.main()
