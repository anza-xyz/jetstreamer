import copy
import unittest

from scripts import select_historical_performance_candidate as select


def policy() -> dict[str, object]:
    return {
        "schema": select.POLICY_SCHEMA,
        "results_manifest_sha256": "a" * 64,
        "fixed_environment": {"target_slot": 100},
        "candidate_throughput_floors_relative_to_control": {
            "slots_per_second": 0.98,
            "transactions_per_second": 0.98,
            "account_updates_per_second": 0.98,
        },
        "candidate_resource_nonregression_ceilings_relative_to_control": {
            "effective_cpu_cores": 1.10,
            "physical_scratch_bytes": 1.02,
            "apparent_scratch_bytes": 1.02,
            "regular_files": 1.02,
            "worker_vmas": 1.02,
        },
        "material_storage_win": {"minimum_relative_reduction": 0.05},
    }


def variant(name: str, *, scale: float = 1.0, scratch_scale: float = 1.0) -> dict[str, object]:
    return {
        "name": name,
        "unit": {
            "load_state": "loaded",
            "active_state": "inactive",
            "result": "success",
            "main_pid": 0,
            "restarts": 0,
        },
        "performance": {
            "final_progress": {"slot": 101},
            "progress_rates": {
                "slots_per_second": 4.0 * scale,
                "transactions_per_second": 2000.0 * scale,
                "account_updates_per_second": 6000.0 * scale,
            },
            "effective_cpu_cores": 2.0 * scale,
        },
        "guard": {"samples": 10, "maximum_worker_vmas": int(300000 * scratch_scale)},
        "scratch": {
            "physical_bytes": int(100000000000 * scratch_scale),
            "apparent_bytes": int(110000000000 * scratch_scale),
            "regular_files": int(300000 * scratch_scale),
        },
        "accounts_state": {
            "appendvec_store_fanout": {
                "recognized_appendvec_files": int(250000 * scratch_scale),
                "mean_stores_per_slot": 16.0 * scratch_scale,
            }
        },
        "wave_metrics": {"waves": 100},
    }


def results(*, candidate_scale: float = 1.0, scratch_scale: float = 0.8) -> dict[str, object]:
    return {
        "schema": select.RESULT_SCHEMA,
        "manifest_sha256": "a" * 64,
        "remote_mutations": False,
        "r2_mutations": False,
        "evidence": {
            "target_slot": 100,
            "selection_authorized": False,
            "variants": [
                variant(select.CONTROL),
                variant(
                    select.CANDIDATE,
                    scale=candidate_scale,
                    scratch_scale=scratch_scale,
                ),
            ],
        },
    }


class HistoricalPerformanceSelectionTest(unittest.TestCase):
    def evaluate(self, policy_payload=None, result_payload=None):
        return select.evaluate_selection(
            policy() if policy_payload is None else policy_payload,
            results() if result_payload is None else result_payload,
            policy_path="/policy.json",
            policy_sha256="b" * 64,
            results_path="/results.json",
            results_sha256="c" * 64,
        )

    def test_selects_store8_when_every_gate_passes(self) -> None:
        receipt = self.evaluate()
        self.assertEqual(receipt["selected_variant"], select.CANDIDATE)
        self.assertTrue(receipt["candidate_selected"])
        self.assertTrue(receipt["material_storage_win"])
        self.assertTrue(all(receipt["throughput_gates"].values()))
        self.assertTrue(all(receipt["resource_gates"].values()))
        self.assertFalse(receipt["qualification_launch_authorized"])
        self.assertFalse(receipt["publication_authorized"])

    def test_selects_control_below_throughput_floor(self) -> None:
        receipt = self.evaluate(result_payload=results(candidate_scale=0.97))
        self.assertEqual(receipt["selected_variant"], select.CONTROL)
        self.assertFalse(receipt["candidate_selected"])
        self.assertFalse(all(receipt["throughput_gates"].values()))

    def test_selects_control_without_material_storage_win(self) -> None:
        receipt = self.evaluate(result_payload=results(scratch_scale=0.97))
        self.assertEqual(receipt["selected_variant"], select.CONTROL)
        self.assertFalse(receipt["material_storage_win"])

    def test_fails_closed_on_manifest_or_terminal_mismatch(self) -> None:
        for mutate in ("manifest", "restarts", "target", "variant"):
            payload = results()
            if mutate == "manifest":
                payload["manifest_sha256"] = "d" * 64
            elif mutate == "restarts":
                payload["evidence"]["variants"][1]["unit"]["restarts"] = 1
            elif mutate == "target":
                payload["evidence"]["variants"][1]["performance"]["final_progress"]["slot"] = 99
            else:
                payload["evidence"]["variants"][1]["name"] = "other"
            with self.subTest(mutate=mutate), self.assertRaises(select.SelectionError):
                self.evaluate(result_payload=payload)

    def test_fails_closed_on_invalid_denominator_or_mutation_claim(self) -> None:
        payload = results()
        payload["evidence"]["variants"][0]["scratch"]["physical_bytes"] = 0
        with self.assertRaises(select.SelectionError):
            self.evaluate(result_payload=payload)
        payload = results()
        payload["r2_mutations"] = True
        with self.assertRaises(select.SelectionError):
            self.evaluate(result_payload=payload)


if __name__ == "__main__":
    unittest.main()
