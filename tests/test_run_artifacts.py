from __future__ import annotations

import math
import unittest

from ingest.common.run_artifacts import json_ready


class _DummyScalar:
    def __init__(self, value: float) -> None:
        self._value = value

    def item(self) -> float:
        return self._value


class RunArtifactsTests(unittest.TestCase):
    def test_json_ready_replaces_non_finite_floats_with_none(self) -> None:
        payload = {
            "previous_fingerprint": math.nan,
            "nested": [1.0, math.inf, -math.inf],
        }

        self.assertEqual(
            json_ready(payload),
            {
                "previous_fingerprint": None,
                "nested": [1.0, None, None],
            },
        )

    def test_json_ready_replaces_scalar_item_nan_with_none(self) -> None:
        self.assertIsNone(json_ready(_DummyScalar(math.nan)))


if __name__ == "__main__":
    unittest.main()
