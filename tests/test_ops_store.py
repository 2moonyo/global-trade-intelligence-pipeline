from __future__ import annotations

import json
import math
import unittest

from warehouse.ops_store import _json_text, _normalize_bigquery_row


class OpsStoreTests(unittest.TestCase):
    def test_json_text_replaces_non_finite_values(self) -> None:
        payload = {"previous_fingerprint": math.nan, "nested": [1.0, math.inf]}

        self.assertEqual(
            json.loads(_json_text(payload)),
            {
                "nested": [1.0, None],
                "previous_fingerprint": None,
            },
        )

    def test_normalize_bigquery_row_replaces_non_finite_values(self) -> None:
        normalized = _normalize_bigquery_row(
            {
                "artifact_type": "manifest",
                "payload_json": {"previous_fingerprint": math.nan},
            }
        )

        self.assertEqual(
            json.loads(normalized["payload_json"]),
            {"previous_fingerprint": None},
        )
        self.assertIn("recorded_at", normalized)


if __name__ == "__main__":
    unittest.main()
