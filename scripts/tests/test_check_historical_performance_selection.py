import copy
import unittest

from scripts import check_historical_performance_selection as gate


POLICY_SHA256 = "a" * 64
SELECTOR_SHA256 = "b" * 64


def receipt(variant: str = "waves-store8-t16") -> dict[str, object]:
    return {
        "schema": gate.RECEIPT_SCHEMA,
        "selected_variant": variant,
        "candidate_selected": variant == "waves-store8-t16",
        "policy_sha256": POLICY_SHA256,
        "selector": {"path": "/sealed/selector", "sha256": SELECTOR_SHA256},
        "selection_authorized": True,
        "qualification_launch_authorized": False,
        "publication_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }


class HistoricalPerformanceSelectionGateTest(unittest.TestCase):
    def validate(self, payload=None, expected="waves-store8-t16") -> None:
        gate.validate_selection(
            receipt() if payload is None else payload,
            expected_variant=expected,
            expected_policy_sha256=POLICY_SHA256,
            expected_selector_sha256=SELECTOR_SHA256,
        )

    def test_accepts_exact_candidate_selection(self) -> None:
        self.validate()

    def test_accepts_exact_control_selection(self) -> None:
        self.validate(receipt("waves-control-t16"), "waves-control-t16")

    def test_rejects_wrong_variant_or_digest(self) -> None:
        with self.assertRaises(gate.SelectionGateError):
            self.validate(expected="waves-control-t16")
        payload = receipt()
        payload["policy_sha256"] = "c" * 64
        with self.assertRaises(gate.SelectionGateError):
            self.validate(payload)
        payload = receipt()
        payload["selector"] = {"sha256": "c" * 64}
        with self.assertRaises(gate.SelectionGateError):
            self.validate(payload)

    def test_rejects_authorization_or_decision_inconsistency(self) -> None:
        for field, value in (
            ("selection_authorized", False),
            ("qualification_launch_authorized", True),
            ("publication_authorized", True),
            ("remote_mutations", True),
            ("r2_mutations", True),
            ("candidate_selected", False),
        ):
            payload = copy.deepcopy(receipt())
            payload[field] = value
            with self.subTest(field=field), self.assertRaises(gate.SelectionGateError):
                self.validate(payload)


if __name__ == "__main__":
    unittest.main()
