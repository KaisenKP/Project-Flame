from __future__ import annotations

import traceback
import unittest

from services.startup_diagnostics import PHASE_RUNTIME, PHASE_STARTUP, STATUS_FAIL, StartupDiagnostics


class DiagnosticsRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.d = StartupDiagnostics()

    def _err(self, msg: str) -> Exception:
        try:
            raise RuntimeError(msg)
        except RuntimeError as exc:
            return exc

    def test_startup_stage_failure_is_counted(self) -> None:
        self.d.record_failure(stage_name="setup_hook", summary="boom", exception=self._err("boom"), traceback_text="tb", fatal=False)
        self.assertEqual(self.d.counts()["startup_errors"], 1)

    def test_extension_failure_is_counted(self) -> None:
        self.d.capture_exception(self._err("ext"), phase=PHASE_STARTUP, subsystem="cogs", source="extension_load", category="extension")
        self.assertEqual(len([e for e in self.d.entries if e.source == "extension_load"]), 1)

    def test_slash_and_prefix_failures_counted(self) -> None:
        self.d.capture_exception(self._err("slash"), subsystem="interactions", source="tree.on_error", category="app_command")
        self.d.capture_exception(self._err("prefix"), subsystem="commands", source="on_command_error", category="prefix_command")
        self.assertEqual(self.d.counts()["errors"], 2)

    def test_button_and_modal_failures_counted(self) -> None:
        self.d.capture_exception(self._err("button"), subsystem="views", source="view.on_error", category="view")
        self.d.capture_exception(self._err("modal"), subsystem="modals", source="modal.on_error", category="modal")
        self.assertEqual(len(self.d.entries), 2)

    def test_background_task_and_business_and_leaderboard_and_db_failures(self) -> None:
        self.d.capture_exception(self._err("task"), subsystem="tasks", source="background_task", category="task", task_name="loop")
        self.d.capture_exception(self._err("biz"), subsystem="business", source="income_update", category="business")
        self.d.capture_exception(self._err("lb"), subsystem="leaderboard", source="refresh", category="leaderboard")
        self.d.capture_exception(self._err("db"), subsystem="database", source="commit", category="database")
        self.assertEqual(len(self.d.entries), 4)

    def test_sys_and_asyncio_sources_counted(self) -> None:
        self.d.capture_exception(self._err("sys"), subsystem="process", source="sys.excepthook", category="unhandled")
        self.d.capture_exception(self._err("loop"), subsystem="tasks", source="asyncio.loop", category="asyncio")
        self.assertEqual(len(self.d.entries), 2)

    def test_duplicate_capture_does_not_double_count(self) -> None:
        exc = self._err("same")
        self.d.capture_exception(exc, subsystem="commands", source="same", category="command")
        self.d.capture_exception(exc, subsystem="commands", source="same", category="command")
        self.assertEqual(len(self.d.entries), 1)

    def test_traceback_attached_to_failure(self) -> None:
        try:
            raise ValueError("trace")
        except ValueError as exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            self.d.capture_exception(exc, subsystem="test", source="trace", category="test", traceback_text=tb)
        self.assertIn("ValueError", self.d.entries[0].traceback_text or "")

    def test_summary_counts_derived_from_entries(self) -> None:
        self.d.capture_exception(self._err("r1"), phase=PHASE_RUNTIME)
        self.d.capture_exception(self._err("s1"), phase=PHASE_STARTUP)
        self.assertEqual(self.d.counts()["errors"], 2)
        self.assertEqual(self.d.counts()["runtime_errors"], 1)
        self.assertEqual(self.d.counts()["startup_errors"], 1)

    def test_errors_and_tracebacks_views_use_same_entries(self) -> None:
        self.d.capture_exception(self._err("view"), subsystem="views", source="view")
        errs = self.d.render_entries_embeds(status=STATUS_FAIL, title="Errors")
        tbs = self.d.render_traceback_embeds()
        self.assertTrue(errs and tbs)
        self.assertIn(self.d.entries[0].id, tbs[0].description or "")


if __name__ == "__main__":
    unittest.main()
