from __future__ import annotations

import unittest

from warehouse.run_dataset_batch import _normalize_manifest_task_status


class RunDatasetBatchTests(unittest.TestCase):
    def test_manifest_success_status_is_completed(self) -> None:
        self.assertEqual(_normalize_manifest_task_status("success"), "completed")

    def test_existing_successful_terminal_statuses_are_completed(self) -> None:
        self.assertEqual(_normalize_manifest_task_status("loaded"), "completed")
        self.assertEqual(_normalize_manifest_task_status("planned"), "completed")
        self.assertEqual(
            _normalize_manifest_task_status("no_op_all_selected_assets_already_loaded"),
            "completed",
        )


if __name__ == "__main__":
    unittest.main()
