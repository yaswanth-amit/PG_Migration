"""Tests for services.py and the Streamlit page. The page runs headless in offline mode: no browser,
no database and no migration process is started.
Run with:  python test_ui.py     (or: pytest test_ui.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

os.environ["PG_MIGRATION_UI_OFFLINE"] = "1"                 # before anything reads it

from streamlit.testing.v1 import AppTest

from pipeline import services
from pipeline import ui_components as ui
from pipeline.load import REPORT_DIR
from pipeline.mapping_config import load_mappings

MS = load_mappings()
COP = "component_operation_pricing"
APP = str(Path(__file__).resolve().parents[1] / "ui_app.py")


def app():
    return AppTest.from_file(APP, default_timeout=60).run()


def button(at, label):
    return next(b for b in at.button if b.label == label)


def errors_text(at):
    return " ".join(e.value for e in at.error)


# ---------------------------------------------------------------- services
def test_run_command_for_all_tables_in_rehearsal():
    cmd = services.build_run_command(None, "rehearsal")
    assert cmd[0] == sys.executable and cmd[2].endswith("main.py")
    assert cmd[3:] == ["run", "--tables", "all", "--mode", "rehearsal"]


def test_run_command_for_a_real_run_carries_the_confirmation_and_options():
    cmd = services.build_run_command(["a", "b"], "real", "tpm_target", datetime(2026, 11, 3), datetime(2026, 11, 18),
                                     days=7, max_windows=0, resume=False)
    tail = cmd[3:]
    assert tail[:4] == ["run", "--tables", "a,b", "--mode"] and "real" in tail
    assert tail[tail.index("--confirm-target") + 1] == "tpm_target"
    assert tail[tail.index("--from") + 1] == "2026-11-03" and tail[tail.index("--to") + 1] == "2026-11-18"
    assert tail[tail.index("--days") + 1] == "7" and tail[tail.index("--max-windows") + 1] == "0"
    assert "--no-resume" in tail


def test_a_rehearsal_command_never_contains_a_confirmation():
    assert "--confirm-target" not in services.build_run_command(None, "rehearsal", "tpm_target")


def test_status_labels():
    assert services.status_label(None).startswith("—")
    assert "migrated" in services.status_label({"status": "completed"})
    assert "held back" in services.status_label({"status": "completed_with_exceptions"})
    assert "failed" in services.status_label({"status": "failed"})


def test_window_progress_counts_finished_windows():
    windows = [{"status": "completed"}, {"status": "completed_with_exceptions"}, {"status": "running"}, {"status": "pending"}]
    assert services.window_progress(windows) == (2, 4)


def test_table_rows_follow_the_load_order_and_show_the_dependencies():
    rows = services.mapping_table_rows(MS, {})
    assert [r["name"] for r in rows] == MS.load_order
    cop = next(r for r in rows if r["name"] == COP)
    assert "machine_information" in cop["needs"] and cop["status"].startswith("—")
    assert next(r for r in rows if r["name"] == "autodata")["windowed"] is True


def test_only_files_inside_the_reports_folder_can_be_downloaded():
    outside = Path(tempfile.mkdtemp()) / "secret.csv"
    outside.write_text("x", encoding="utf-8")
    assert services.read_exceptions_file(str(outside)) is None
    assert services.read_exceptions_file(str(REPORT_DIR / ".." / ".." / "secret.csv")) is None
    assert services.read_exceptions_file(None) is None


def test_offline_mode_never_touches_a_database():
    rows, error = services.fetch_status()
    assert rows == {} and "offline" in error
    assert services.mark_running_as_stopped() == "offline mode"


def test_connection_info_has_no_secrets():
    info = services.connection_info()
    assert "password" not in str(info).lower() and "pwd" not in str(info).lower()


# ---------------------------------------------------------------- the page
def test_the_page_starts_without_errors_and_shows_every_table():
    at = app()
    assert not at.exception
    assert len([c for c in at.checkbox if c.key and c.key.startswith("sel_")]) == 26
    assert len(at.tabs) == 4


def test_select_all_and_clear():
    at = app()
    button(at, "Select all").click().run()
    assert all(c.value for c in at.checkbox if c.key and c.key.startswith("sel_"))
    assert not at.error                                             # the complete set is a valid selection
    button(at, "Clear").click().run()
    assert not any(c.value for c in at.checkbox if c.key and c.key.startswith("sel_"))


def test_cop_alone_shows_the_dependency_error_and_blocks_the_start_button():
    at = app()
    at.checkbox(key=f"sel_{COP}").check().run()
    text = errors_text(at)
    assert COP in text and "machine_information" in text and "component_information" in text
    assert button(at, "Start rehearsal").disabled


def test_add_required_tables_fixes_the_selection():
    at = app()
    at.checkbox(key=f"sel_{COP}").check().run()
    button(at, "Add required tables").click().run()
    assert not at.error
    for needed in ("machine_information", "component_information", "customer_information"):
        assert at.checkbox(key=f"sel_{needed}").value
    assert not button(at, "Start rehearsal").disabled


def test_nothing_selected_cannot_start():
    at = app()
    assert button(at, "Start rehearsal").disabled


def test_a_real_run_needs_the_database_name_and_the_tracking_schema():
    at = app()
    at.checkbox(key="sel_customer_information").check().run()
    at.radio(key="mode").set_value("Real migration").run()
    assert button(at, "Start migration").disabled                    # nothing typed yet
    at.text_input(key="confirm_target").set_value("wrong_name").run()
    assert button(at, "Start migration").disabled
    # even the right name cannot start it here: offline mode means there is no tracking schema
    assert any("tracking schema" in c.value for c in at.caption)


def test_autodata_shows_the_window_options():
    at = app()
    assert not [n for n in at.number_input if n.key == "days"]
    at.checkbox(key="sel_autodata").check().run()
    assert any(n.key == "days" and n.value == 0 for n in at.number_input)            # 0 = logical months
    assert any(n.key == "max_windows" and n.value == 2 for n in at.number_input)      # rehearsal default


def test_the_mapping_tab_shows_the_columns_of_the_chosen_table():
    at = app()
    at.selectbox(key="mapping_choice").set_value("customer_information").run()
    assert not at.exception
    assert any("customerinformation" in m.value for m in at.markdown)
    frames = [d.value for d in at.dataframe]
    assert any("target column" in list(f.columns) and "customer_id" in f["target column"].tolist() for f in frames)


# ---------------------------------------------------------------- live progress and stages
def _snapshot(status="running", phase="migrating"):
    import time
    now = time.time()
    return {"run_id": "R1", "mode": "rehearsal", "status": status, "phase": phase, "started_ts": now - 100,
            "updated_ts": now, "finished_ts": None,
            "tables": [
                {"name": "a", "status": "completed", "rows_total": 100, "rows_read": 100, "rows_loaded": 100,
                 "rows_excepted": 0, "stage": None, "windows_done": 0, "windows_total": 0, "error": None},
                {"name": "b", "status": "running", "rows_total": 300, "rows_read": 100, "rows_loaded": 90,
                 "rows_excepted": 10, "stage": "load", "windows_done": 0, "windows_total": 0, "error": None},
                {"name": "c", "status": "pending", "rows_total": 100, "rows_read": 0, "rows_loaded": 0,
                 "rows_excepted": 0, "stage": None, "windows_done": 0, "windows_total": 0, "error": None}]}


def test_summary_gives_percentage_speed_eta_and_the_active_stage():
    s = services.summarize_progress(_snapshot())
    assert s["rows_total"] == 500 and s["rows_read"] == 200 and s["rows_loaded"] == 190
    assert abs(s["pct"] - 0.4) < 1e-9 and s["tables_done"] == 1
    assert s["active_table"] == "b" and s["active_stage"] == "load"
    assert s["eta"] and s["rate"] > 0


def test_summary_without_counts_falls_back_to_finished_tables():
    snap = _snapshot()
    for t in snap["tables"]:
        t["rows_total"] = None
    s = services.summarize_progress(snap)
    assert s["rows_total"] is None and abs(s["pct"] - 1 / 3) < 1e-9


def test_activity_uses_the_process_when_known_and_freshness_otherwise():
    snap = _snapshot()
    assert services.is_active(snap, process_alive=True) and not services.is_active(snap, process_alive=False)
    assert services.is_active(snap)
    assert not services.is_active(snap, now=snap["updated_ts"] + services.STALE_SECONDS + 1)
    assert not services.is_active(_snapshot("completed"))


def test_progress_reporter_writes_stages_counts_and_survives_a_bad_path():
    from pipeline.progress import ProgressReporter
    path = Path(tempfile.mkdtemp()) / "p.json"
    r = ProgressReporter(path, min_interval=0)
    r.start_run("R", "rehearsal", ["a"])
    r.phase("counting"); r.set_totals({"a": 10})
    r.table_started("a"); r.table_stage("a", "transform"); r.table_progress("a", 4, 1)
    snap = services.read_progress(path)
    assert snap["phase"] == "counting" and snap["tables"][0]["stage"] == "transform"
    assert snap["tables"][0]["rows_read"] == 4 and snap["tables"][0]["rows_loaded"] == 3
    r.table_finished("a", "completed", 10, 10, 0)
    r.finish_run("completed")
    snap = services.read_progress(path)
    assert snap["status"] == "completed" and snap["tables"][0]["stage"] is None
    ProgressReporter(path.parent).start_run("R", "real", ["a"])      # a directory as the file: must not raise


def test_html_shows_rows_stages_and_escapes_names():
    snap = _snapshot()
    html = ui.live_card(snap, services.summarize_progress(snap), 0.1)
    assert "190" in html and "Transform" in html and "Load" in html and "Migrating b" in html and "LIVE" in html
    assert "\n" not in html                                         # one line: markdown would show indented lines as code
    snap["tables"][0]["name"] = "<script>x</script>"
    assert "<script>" not in ui.table_rows(snap)
    done = dict(_snapshot("completed", "done"), finished_ts=1.0)
    assert "Rehearsal complete" in ui.live_card(done, services.summarize_progress(done))


def test_the_command_carries_the_progress_file():
    cmd = services.build_run_command(None, "rehearsal", progress_file="p.json")
    assert cmd[cmd.index("--progress-file") + 1] == "p.json"


def test_a_run_reports_progress_through_every_stage():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import test_runner as tr
    from pipeline.progress import ProgressReporter
    path = Path(tempfile.mkdtemp()) / "run.json"
    h = tr.Harness()
    h.run(["customer_information"], progress=ProgressReporter(path, min_interval=0))
    snap = services.read_progress(path)
    assert snap["status"] == "completed" and snap["phase"] == "done"
    t = snap["tables"][0]
    assert t["name"] == "customer_information" and t["status"] == "completed"
    assert t["rows_read"] == 2 and t["rows_total"] == 2 and t["stage"] is None


def test_a_date_range_outside_the_data_is_detected():
    from datetime import date
    low, high = date(2026, 11, 3), date(2026, 11, 18)
    assert not services.range_has_data(date(2026, 11, 19), date(2026, 11, 19), low, high)     # today: the old default
    assert services.range_has_data(date(2026, 11, 1), date(2026, 11, 3), low, high)
    assert services.range_has_data(date(2026, 11, 19), date(2026, 11, 19), None, None)        # unknown: do not block
    assert services.fetch_source_range(MS.tables["autodata"]) == (None, None)                 # offline


def test_a_windowed_table_that_read_nothing_gets_a_hint():
    snap = _snapshot("completed", "done")
    snap["tables"][0].update(name="autodata", rows_read=0, windows_total=2, windows_done=2)
    assert "No rows were read" in ui.table_rows(snap)


def test_a_dead_process_in_the_progress_file_means_the_run_is_not_active():
    import os
    snap = dict(_snapshot(), pid=os.getpid())
    assert services.pid_is_running(os.getpid()) is True and services.is_active(snap)
    assert services.pid_is_running(None) is None
    dead = dict(_snapshot(), pid=2_000_000_000)                        # no such process, though the file is fresh
    assert services.pid_is_running(2_000_000_000) is False and not services.is_active(dead)


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok    {name}")
        except Exception as e:                    # noqa: BLE001 - a test runner reports every kind of failure
            failed += 1
            print(f"  FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    raise SystemExit(1 if failed else 0)
