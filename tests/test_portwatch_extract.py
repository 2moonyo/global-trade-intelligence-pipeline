from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import Mock, patch

from ingest.portwatch.portwatch_extract import (
    PortWatchConfig,
    _request_json,
    fetch_daily_for_date,
)


class PortWatchExtractTests(unittest.TestCase):
    def test_fetch_daily_for_date_uses_arcgis_timestamp_filter(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_request_json(_session: object, _url: str, params: dict, _cfg: PortWatchConfig) -> dict:
            calls.append(dict(params))
            return {
                "features": [
                    {
                        "attributes": {
                            "date": 1577836800000,
                            "portid": "chokepoint1",
                            "portname": "Suez Canal",
                            "n_total": 58,
                        }
                    }
                ]
            }

        cfg = PortWatchConfig(page_size=2000)
        with patch("ingest.portwatch.portwatch_extract._request_json", side_effect=fake_request_json):
            df = fetch_daily_for_date(["chokepoint1"], date(2020, 1, 1), cfg)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "year"], 2020)
        self.assertEqual(df.loc[0, "month"], "01")
        self.assertEqual(df.loc[0, "day"], "01")

        where = str(calls[0]["where"])
        self.assertIn("portid IN ('chokepoint1')", where)
        self.assertIn("date >= timestamp '2020-01-01 00:00:00'", where)
        self.assertIn("date < timestamp '2020-01-02 00:00:00'", where)
        self.assertNotIn("1577836800000", where)

    def test_request_json_raises_on_arcgis_error_payload(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "error": {
                "code": 400,
                "message": "",
                "details": ["Unable to perform query. Please check your parameters."],
            }
        }
        session = Mock()
        session.get.return_value = response
        cfg = PortWatchConfig(max_retries=3, retry_backoff_s=0)

        with self.assertRaisesRegex(RuntimeError, "ArcGIS API error 400"):
            _request_json(session, "https://example.test/query", {"where": "bad"}, cfg)

        self.assertEqual(session.get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
