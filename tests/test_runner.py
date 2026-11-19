"""Offline tests for runner.py. Extract, transform, load and the databases are replaced by recording fakes,
so these tests check the runner's own logic. No database is touched.
Run with:  python test_runner.py     (or: pytest test_runner.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import tempfile
from contextlib import ExitStack, contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import polars as pl

from pipeline import extract as ex
from pipeline import load as ld
from pipeline import log_setup
from pipeline import runner
from pipeline import tracking
from pipeline.mapping_config import load_mappings
from pipeline.transform import TransformResult

MS = load_mappings()
COP = "component_operation_pricing"
AUTODATA_PARENTS = {"machine_information", "component_information", COP, "employee_information",
                    "down_code_information"}
JAN1, JAN16, JAN31 = datetime(2026, 1, 1), datetime(2026, 1, 16), datetime(2026, 1, 31)


class FakeTgt:
    def __init__(self):
        self.events = []

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")

    def execute(self, query):
        self.events.append("execute")

    @contextmanager
    def transaction(self):
        self.events.append("savepoint")
        try:
            yield
        except Exception:
            self.events.append("savepoint_rollback")
            raise
        self.events.append("release")


class RecordingTracker:
    def __init__(self, completed=(), windows=()):
        self.calls = []
        self._completed = set(completed)
        self._windows = set(windows)

    def completed_tables(self):
        return set(self._completed)

    def completed_windows(self, table):
        return set(self._windows)

    def start_run(self, mode, tables, source_db, target_db, note=None):
        self.calls.append(("start_run", mode, list(tables)))
        return "RUN1"

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self.calls.append((name,) + args)
        return record

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c for c in self.calls if c[0] == name]


class Harness:
    """Patches everything the runner talks to and records what happened."""

    def __init__(self, fail_table=None, fail_window=None, rows_loaded=None, combined_count=6,
                 fail_references=False):
        self.loaded = []            # (table, kwargs)
        self.windows_loaded = []    # (window_no, start, end, rehearse)
        self.asserted_empty = []
        self.hook_calls = []
        self.fail_table, self.fail_window = fail_table, fail_window
        self.rows_loaded = rows_loaded
        self.combined_count = combined_count
        self.fail_references = fail_references
        self.target_has_window = True       # does the target still hold rows of a window the history calls completed?
        self.tgt = FakeTgt()
        self.cache = mock.MagicMock()

    # ---- fakes
    def extract_batches(self, conn, m, where=None, params=(), batch_size=0, icd_lookback_seconds=None, icd_cycles=None):
        def gen():
            yield pl.DataFrame({"x": [1, 2]})
        return gen()

    def transform_batch(self, batch, m, ctx):
        empty = pl.DataFrame(schema={"reason": pl.String, "column": pl.String, "key": pl.String})
        return TransformResult(rows=batch, exceptions=empty)

    def load_table(self, conn, m, ctx, results, **kwargs):
        if m.name == self.fail_table:
            raise RuntimeError("boom")
        rows = sum(r.rows.height for r in results)
        summary = ld.LoadSummary(table=m.name, rows_loaded=rows if self.rows_loaded is None else self.rows_loaded)
        self.loaded.append((m.name, kwargs))
        if kwargs.get("before_commit") and not kwargs.get("rehearse"):
            kwargs["before_commit"](conn, summary)
        return summary

    def load_window(self, conn, m, ctx, results, window_no, start, end, **kwargs):
        if window_no == self.fail_window:
            raise RuntimeError("window boom")
        rows = sum(r.rows.height for r in results)
        summary = ld.LoadSummary(table=m.name, rows_loaded=rows, rows_deleted=0)
        self.windows_loaded.append((window_no, start, end, kwargs.get("rehearse", False)))
        if kwargs.get("before_commit") and not kwargs.get("rehearse"):
            kwargs["before_commit"](conn, summary)
        return summary

    def count_source_rows(self, conn, m, where=None, params=()):
        return self.combined_count if where and " OR " in where else 2

    def write_table_result(self, conn, run_id, name, rows_read, rows_loaded, rows_excepted, misses=None, file=None):
        self.hook_calls.append(("table", name, rows_read, rows_loaded))

    def write_window_result(self, conn, run_id, name, number, rows_read, rows_loaded, rows_excepted):
        self.hook_calls.append(("window", number, rows_read, rows_loaded))

    def references(self, conn, name, *a, **k):
        if self.fail_references:
            raise RuntimeError("source offline")
        return pl.DataFrame({"DownId": ["NO_DATA"]})

    def window_has_rows(self, conn, m, start, end):
        return self.target_has_window

    def assert_target_empty(self, conn, schema, table):
        self.asserted_empty.append(table)

    # ---- run
    def run(self, tables, mode="rehearsal", tracker=None, **option_kwargs):
        tracker = tracker if tracker is not None else RecordingTracker()
        options = runner.RunOptions(tables=tables, mode=mode, report_dir=Path(tempfile.mkdtemp()), **option_kwargs)
        cache = self.cache
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(ex, "extract_batches", self.extract_batches))
            stack.enter_context(mock.patch.object(ex, "extract_reference_table", self.references))
            stack.enter_context(mock.patch.object(ex, "source_time_range", lambda *a: (JAN1, JAN31 - (JAN31 - datetime(2026, 1, 20)))))
            stack.enter_context(mock.patch.object(ex, "count_source_rows", self.count_source_rows))
            stack.enter_context(mock.patch.object(ex, "compute_icd_lookback", lambda *a: 100))
            stack.enter_context(mock.patch.object(ex, "extract_all_icd_cycles", lambda *a: pl.DataFrame()))
            stack.enter_context(mock.patch.object(ld, "load_table", self.load_table))
            stack.enter_context(mock.patch.object(ld, "load_window", self.load_window))
            stack.enter_context(mock.patch.object(ld, "assert_target_empty", self.assert_target_empty))
            stack.enter_context(mock.patch.object(ld, "window_has_rows", self.window_has_rows))
            stack.enter_context(mock.patch.object(tracking, "write_table_result", self.write_table_result))
            stack.enter_context(mock.patch.object(tracking, "write_window_result", self.write_window_result))
            stack.enter_context(mock.patch.object(runner, "transform_batch", self.transform_batch))
            stack.enter_context(mock.patch.object(runner, "read_target_schema", lambda *a: {}))
            stack.enter_context(mock.patch.object(runner, "LookupCache", lambda *a: cache))
            stack.enter_context(mock.patch.object(log_setup, "LOG_DIR", Path(tempfile.mkdtemp())))
            stack.enter_context(mock.patch.object(runner, "setup_logging", lambda run_id: log_setup.setup_logging(run_id, Path(tempfile.mkdtemp()))))
            self.result = runner.run_migration(MS, options, src_conn=object(), tgt_conn=self.tgt, tracker=tracker)
        self.tracker = tracker
        return self.result


def status_of(result):
    return {o.name: o.status for o in result.outcomes}


# ---------------------------------------------------------------- selection and modes
def test_an_invalid_selection_is_refused_before_anything_runs():
    h = Harness()
    tracker = RecordingTracker()
    try:
        h.run([COP], tracker=tracker)
    except runner.SelectionError as e:
        assert "machine_information" in str(e) and "component_information" in str(e)
        assert tracker.calls == [] and h.loaded == [] and h.tgt.events == []
    else:
        raise AssertionError("cop without its parents must be refused")


def test_a_selection_is_allowed_when_the_parents_were_migrated_earlier():
    h = Harness()
    r = h.run([COP], tracker=RecordingTracker(completed={"machine_information", "component_information"}))
    assert status_of(r) == {COP: "completed"}


def test_an_unknown_mode_is_refused():
    try:
        Harness().run(["customer_information"], mode="production")
    except ValueError:
        return
    raise AssertionError("an unknown mode must be refused")


def test_the_default_mode_is_a_rehearsal():
    assert runner.RunOptions().mode == "rehearsal"


# ---------------------------------------------------------------- rehearsal
def test_rehearsal_runs_parent_before_child_in_one_transaction_and_rolls_back():
    h = Harness()
    r = h.run(["component_information", "customer_information"])
    assert [name for name, _ in h.loaded] == ["customer_information", "component_information"]
    assert all(k["rehearse"] and k["manage_transaction"] is False for _, k in h.loaded)
    assert h.tgt.events == ["commit", "execute", "savepoint", "release", "savepoint", "release", "rollback"]
    assert r.status == "completed" and r.mode == "rehearsal"
    assert h.tracker.of("finish_run") == [("finish_run", "RUN1", "completed")]
    assert not h.hook_calls                              # nothing is recorded inside a transaction that is rolled back


def test_a_rehearsal_rolls_back_even_when_a_table_fails():
    h = Harness(fail_table="customer_information")
    h.run(["customer_information"])
    assert h.tgt.events[-1] == "rollback"


# ---------------------------------------------------------------- real run
def test_real_run_commits_per_table_and_records_inside_the_transaction():
    h = Harness()
    r = h.run(["customer_information", "down_category"], mode="real")
    assert [name for name, _ in h.loaded] == ["customer_information", "down_category"]
    assert all(k.get("before_commit") and not k.get("rehearse") for _, k in h.loaded)
    assert "rollback" not in h.tgt.events and "savepoint" not in h.tgt.events
    assert h.hook_calls == [("table", "customer_information", 2, 2), ("table", "down_category", 2, 2)]
    assert r.status == "completed" and h.tracker.of("start_run")[0][1] == "real"


def test_lookup_cache_is_refreshed_after_each_table_so_children_see_the_parent_rows():
    h = Harness()
    h.run(["customer_information", "component_information"])
    assert [c.args[0] for c in h.cache.forget.call_args_list] == ["customer_information", "component_information"]


# ---------------------------------------------------------------- failures
def test_a_failed_parent_blocks_its_children_but_not_independent_tables():
    h = Harness(fail_table="customer_information")
    r = h.run(["customer_information", "component_information", "down_category"])
    assert status_of(r) == {"customer_information": "failed", "component_information": "blocked",
                            "down_category": "completed"}
    assert r.status == "failed"
    assert h.tracker.of("fail_table")[0][1:] == ("RUN1", "customer_information", "RuntimeError: boom")
    assert "customer_information did not complete" in h.tracker.of("block_table")[0][3]
    assert [n for n, _ in h.loaded] == ["down_category"]


def test_rows_that_vanish_without_an_exception_are_an_error():
    h = Harness(rows_loaded=1)                          # 2 rows read, 1 loaded, no exception listed
    r = h.run(["customer_information"])
    assert r.outcomes[0].status == "failed" and "none is listed as an exception" in r.outcomes[0].error


def test_a_database_error_is_logged_without_the_database_message():
    class DbError(Exception):
        sqlstate = "22P02"
        diag = SimpleNamespace(table_name="employee_information", column_name="upassword", constraint_name=None)

    text = runner.safe_error(DbError("invalid input syntax: 'hunter2'"))
    assert "hunter2" not in text and "22P02" in text and "upassword" in text


def test_a_failure_outside_any_table_closes_the_run_as_failed_and_rolls_back():
    h = Harness(fail_references=True)
    tracker = RecordingTracker(completed={"down_category"})
    try:
        h.run(["down_code_information"], tracker=tracker)          # needs PredefinedDownCodeInfo
    except RuntimeError:
        assert tracker.of("finish_run") == [("finish_run", "RUN1", "failed")]
        assert h.tgt.events[-1] == "rollback"
    else:
        raise AssertionError("the failure must propagate")


# ---------------------------------------------------------------- autodata windows
def autodata_tracker(windows=()):
    return RecordingTracker(completed=AUTODATA_PARENTS, windows=windows)


def test_autodata_rehearsal_plans_windows_with_a_null_window_and_loads_each_in_a_savepoint():
    h = Harness()
    r = h.run(["autodata"], tracker=autodata_tracker())
    planned = h.tracker.of("plan_windows")[0][3]
    assert planned == [(JAN1, JAN16), (JAN16, datetime(2026, 1, 31)), (None, None)]
    assert [w[0] for w in h.windows_loaded] == [1, 2, 3] and all(w[3] for w in h.windows_loaded)
    assert h.tgt.events.count("savepoint") == 3 and h.tgt.events[-1] == "rollback"
    assert len(h.tracker.of("finish_window")) == 3
    o = r.outcomes[0]
    assert o.status == "completed" and o.rows_read == 6 and o.windows_done == 3 and o.windows_total == 3


def test_max_windows_limits_a_rehearsal_of_a_big_table():
    h = Harness()
    r = h.run(["autodata"], tracker=autodata_tracker(), max_windows=1)
    assert [w[0] for w in h.windows_loaded] == [1] and r.outcomes[0].windows_total == 1


def test_a_custom_window_size_is_used():
    h = Harness()
    h.run(["autodata"], tracker=autodata_tracker(), days=30, max_windows=5)
    planned = h.tracker.of("plan_windows")[0][3]
    assert planned[0] == (JAN1, datetime(2026, 1, 31))


def test_a_fresh_real_run_requires_an_empty_target():
    h = Harness()
    h.run(["autodata"], mode="real", tracker=autodata_tracker())
    assert h.asserted_empty == ["autodata"]
    assert all(not w[3] for w in h.windows_loaded)                       # not rehearsals
    assert [c[:3] for c in h.hook_calls] == [("window", 1, 2), ("window", 2, 2), ("window", 3, 2)]


def test_a_resumed_run_skips_completed_windows_and_does_not_demand_an_empty_target():
    h = Harness()
    tracker = autodata_tracker(windows={(JAN1, JAN16)})
    r = h.run(["autodata"], mode="real", tracker=tracker)
    assert [w[0] for w in h.windows_loaded] == [2, 3]
    assert h.asserted_empty == []
    assert r.outcomes[0].windows_done == 3


def test_history_is_not_trusted_when_the_target_no_longer_holds_the_window():
    h = Harness()
    h.target_has_window = False                  # the target was emptied after the earlier run
    h.run(["autodata"], mode="real", tracker=autodata_tracker(windows={(JAN1, JAN16)}))
    assert [w[0] for w in h.windows_loaded] == [1, 2, 3]
    assert h.asserted_empty == ["autodata"]


def test_resume_can_be_switched_off():
    h = Harness()
    h.run(["autodata"], mode="real", tracker=autodata_tracker(windows={(JAN1, JAN16)}), resume=False)
    assert [w[0] for w in h.windows_loaded] == [1, 2, 3] and h.asserted_empty == ["autodata"]


def test_a_failing_window_stops_the_table_and_is_recorded():
    h = Harness(fail_window=2)
    r = h.run(["autodata"], mode="real", tracker=autodata_tracker())
    assert [w[0] for w in h.windows_loaded] == [1]                       # window 3 is never attempted
    assert h.tracker.of("fail_window")[0][2:4] == ("autodata", 2)
    assert r.outcomes[0].status == "failed"


def test_windows_that_do_not_cover_the_planned_range_fail_the_table():
    h = Harness(combined_count=7)                                        # the range holds 7 rows, windows counted 6
    r = h.run(["autodata"], tracker=autodata_tracker())
    assert r.outcomes[0].status == "failed" and "counted 6 rows but the planned range holds 7" in r.outcomes[0].error


def test_a_window_whose_read_count_differs_from_the_source_count_fails():
    h = Harness()
    original = h.count_source_rows
    h.count_source_rows = lambda conn, m, where=None, params=(): 3 if where and " OR " not in where else original(conn, m, where, params)
    r = h.run(["autodata"], tracker=autodata_tracker())
    assert r.outcomes[0].status == "failed" and "read 2 rows but the source counts 3" in r.outcomes[0].error


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
