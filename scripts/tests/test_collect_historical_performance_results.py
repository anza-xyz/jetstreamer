import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from scripts import collect_historical_performance_results as collect


class HistoricalPerformanceResultsTest(unittest.TestCase):
    def test_terminal_success_accepts_collected_systemd_metadata(self) -> None:
        for invocation_id, exec_main_code in (("invocation", 1), ("", 0)):
            state = collect.UnitState(
                unit="horizon-perf-epoch202@one.service",
                load_state="loaded",
                active_state="inactive",
                sub_state="dead",
                result="success",
                main_pid=0,
                invocation_id=invocation_id,
                restarts=0,
                exec_main_code=exec_main_code,
                exec_main_status=0,
                cpu_usage_nsec=None,
                memory_peak_bytes=None,
            )
            collect.require_terminal_success(state, "invocation")

    def test_parses_latest_wave_metrics(self) -> None:
        prefix = "historical execution wave metrics: "
        first = (
            prefix
            + "reason=periodic transactions=10 waves=5 singleton_waves=2 "
            "maximum_wave_size=4 size1=2 size2=1 size3=1 size4=1 size5=0 "
            "size6_7=0 size8plus=0"
        )
        second = first.replace("transactions=10", "transactions=20").replace(
            "waves=5", "waves=8"
        )
        result = collect.parse_wave_metrics([first, "noise", second])
        self.assertEqual(result["transactions"], 20)
        self.assertEqual(result["waves"], 8)
        self.assertEqual(result["singleton_wave_fraction"], 0.25)

    def test_parses_guard_maximum_vmas_per_unit(self) -> None:
        units = {"horizon-perf-epoch202@one.service"}
        lines = [
            json.dumps(
                {
                    "schema": collect.GUARD_SCHEMA,
                    "samples": [
                        {"unit": next(iter(units)), "worker_vmas": value}
                    ],
                }
            )
            for value in (100, 120, None, 110)
        ]
        self.assertEqual(
            collect.parse_guard_samples(lines, units)[next(iter(units))],
            {"samples": 4, "maximum_worker_vmas": 120},
        )

    def test_tree_statistics_does_not_follow_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            outside = Path(directory) / "outside"
            root.mkdir()
            outside.mkdir()
            (root / "small").write_bytes(b"a")
            (root / "large").write_bytes(b"b" * (4 * 1024 * 1024 + 1))
            (outside / "hidden").write_bytes(b"secret")
            (root / "link").symlink_to(outside, target_is_directory=True)
            result = collect.tree_statistics(root)
            self.assertEqual(result["regular_files"], 2)
            self.assertEqual(result["symlinks_not_followed"], 1)
            self.assertEqual(result["file_size_histogram"]["le_4_mib"], 1)
            self.assertEqual(result["file_size_histogram"]["gt_4_le_8_mib"], 1)

    def test_validates_successful_canary_receipt(self) -> None:
        payload = {
            "schema": collect.CANARY_SCHEMA,
            "systemd_invocation_id": "invocation",
            "target_slot": 100,
            "target_reached": True,
            "child_return_code": 0,
            "external_signal": None,
            "first_progress": {"slot": 90, "transactions": 10, "account_updates": 20},
            "final_progress": {"slot": 101, "transactions": 40, "account_updates": 80},
            "progress_rates": {"transactions_per_second": 3.0},
            "elapsed_seconds": 10.0,
        }
        result = collect.validate_canary_receipt(payload, 100, "invocation")
        self.assertEqual(result["transaction_delta"], 30)
        self.assertEqual(result["account_update_delta"], 60)
        for key, value in (
            ("target_reached", False),
            ("child_return_code", 1),
            ("external_signal", 15),
        ):
            broken = {**payload, key: value}
            with self.subTest(key=key), self.assertRaises(collect.CollectionError):
                collect.validate_canary_receipt(broken, 100, "invocation")
        with self.assertRaises(collect.CollectionError):
            collect.validate_canary_receipt(payload, 100, "other-invocation")

    def test_result_receipt_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "receipt.json"
            collect.write_json_noclobber(
                path, {"ok": True}, required_uid=root.stat().st_uid
            )
            self.assertEqual(json.loads(path.read_text()), {"ok": True})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                collect.write_json_noclobber(
                    path, {"ok": False}, required_uid=root.stat().st_uid
                )


if __name__ == "__main__":
    unittest.main()
