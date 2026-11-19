"""Offline tests for load.py. A fake connection records what would be sent; no database is touched.
Run with:  python test_load.py     (or: pytest test_load.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl

from pipeline.mapping_config import TableMapping, ColumnRule
from pipeline.target_schema import TargetColumn
from pipeline.transform import TransformContext, TransformResult
from pipeline import load
class FakeCopy:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def write_row(self, row):
        self.store.append(row)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=()):
        text = query if isinstance(query, str) else query.as_string()
        self.conn.statements.append((text, params))
        self.conn.last_text = text
        if "DELETE" in text:
            self.rowcount = 7

    def fetchone(self):
        text = self.conn.last_text
        if text.startswith("SELECT EXISTS"):
            return (self.conn.table_has_rows,)
        if text.startswith("SELECT MAX"):
            return (self.conn.max_value,)
        return (None,)

    def copy(self, query):
        self.conn.statements.append((query.as_string(), ()))
        return FakeCopy(self.conn.copied)


class FakeConn:
    def __init__(self, table_has_rows=False, max_value=None):
        self.statements = []
        self.copied = []
        self.events = []
        self.table_has_rows = table_has_rows
        self.max_value = max_value
        self.last_text = ""

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")


def mapping(name="rework_information"):
    return TableMapping(name=name, src_table="src", tgt_table=name, depends_on=[], columns=[],
                        default={}, unmapped_src=[],
                        raw={"target": {"schema": "master_data"}})


def schema():
    return {
        "rework_no": TargetColumn("rework_no", "integer", False, "nextval('master_data.rework_no_seq'::regclass)"),
        "rework_id": TargetColumn("rework_id", "text", False),
    }


def context(misses=None):
    ctx = TransformContext(load_time=datetime(2026, 1, 1), target_schema=schema())
    ctx.lookup_misses = misses or {}
    return ctx


def result(rows, exceptions=None):
    empty = pl.DataFrame(schema={"reason": pl.String, "column": pl.String, "key": pl.String})
    return TransformResult(rows=pl.DataFrame(rows), exceptions=exceptions if exceptions is not None else empty)


def test_copy_dataframe_writes_every_row_with_a_quoted_copy_statement():
    conn = FakeConn()
    n = load.copy_dataframe(conn, "master_data", "rework_information",
                            pl.DataFrame({"rework_no": [1, 2], "rework_id": ["a", "b"]}))
    assert n == 2 and conn.copied == [(1, "a"), (2, "b")]
    assert conn.statements[0][0] == 'COPY "master_data"."rework_information" ("rework_no", "rework_id") FROM STDIN'


def test_empty_frame_sends_nothing():
    conn = FakeConn()
    assert load.copy_dataframe(conn, "s", "t", pl.DataFrame({"a": []})) == 0
    assert conn.statements == []


def test_non_empty_target_is_refused():
    conn = FakeConn(table_has_rows=True)
    try:
        load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                        report_dir=Path(tempfile.mkdtemp()))
    except load.TargetNotEmptyError:
        assert conn.events[-1] == "rollback" and conn.copied == []
    else:
        raise AssertionError("a non-empty target must be refused")


def test_successful_load_commits_once_and_advances_the_sequence():
    conn = FakeConn(max_value=5)
    summary = load.load_table(conn, mapping(), context(),
                              [result({"rework_no": [1, 5], "rework_id": ["a", "b"]}),
                               result({"rework_no": [3], "rework_id": ["c"]})],
                              report_dir=Path(tempfile.mkdtemp()))
    assert summary.rows_loaded == 3 and not summary.rehearsal
    assert conn.events.count("commit") == 2 and conn.events[-1] == "commit"      # one at the start, one at the end
    setval = [s for s in conn.statements if "setval" in s[0]]
    assert setval == [("SELECT setval(%s::regclass, %s, true)", ("master_data.rework_no_seq", 5))]


def test_rehearsal_rolls_back_instead_of_committing():
    conn = FakeConn(max_value=1)
    summary = load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                              report_dir=Path(tempfile.mkdtemp()), rehearse=True)
    assert summary.rehearsal and summary.rows_loaded == 1
    assert conn.events[-1] == "rollback" and conn.events.count("commit") == 1       # only the one at the start


def test_before_commit_runs_inside_the_transaction_just_before_the_commit():
    conn = FakeConn()
    calls = []

    def hook(c, summary):
        calls.append((list(c.events), summary.rows_loaded))

    load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                    report_dir=Path(tempfile.mkdtemp()), before_commit=hook)
    assert calls == [(["commit"], 1)]            # only the start-of-load commit had happened when the hook ran
    assert conn.events == ["commit", "commit"]


def test_before_commit_is_skipped_for_a_rehearsal_and_a_failing_hook_rolls_back():
    conn = FakeConn()
    called = []
    load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                    report_dir=Path(tempfile.mkdtemp()), rehearse=True, before_commit=lambda c, s: called.append(1))
    assert called == []

    conn2 = FakeConn()

    def boom(c, s):
        raise RuntimeError("tracking write failed")

    try:
        load.load_table(conn2, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                        report_dir=Path(tempfile.mkdtemp()), before_commit=boom)
    except RuntimeError:
        assert conn2.events[-1] == "rollback" and conn2.events.count("commit") == 1
    else:
        raise AssertionError("a failing hook must roll the load back")


def test_rehearsal_does_not_move_sequences():
    conn = FakeConn(max_value=9)
    load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                    report_dir=Path(tempfile.mkdtemp()), rehearse=True)
    assert not [s for s in conn.statements if "setval" in s[0]]


def test_caller_owned_transaction_is_not_committed_or_rolled_back_by_load_table():
    conn = FakeConn()
    load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                    report_dir=Path(tempfile.mkdtemp()), rehearse=True, manage_transaction=False)
    assert conn.events == [] and conn.copied == [(1, "a")]


def window_mapping():
    m = mapping("autodata")
    m.columns = [ColumnRule(dst="st_time", src="sttime", rule="copy")]
    m.raw = {"target": {"schema": "production_core"}, "batching": {"column": "sttime"}}
    return m


def test_window_target_column_is_found_from_the_mapping():
    assert load.window_target_column(window_mapping()) == "st_time"


def test_load_window_deletes_the_window_first_then_inserts_then_commits():
    conn = FakeConn()
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 16)
    summary = load.load_window(conn, window_mapping(), context(), [result({"st_time": [start]})],
                               window_no=2, start=start, end=end, report_dir=Path(tempfile.mkdtemp()))
    texts = [s[0] for s in conn.statements]
    assert texts[0].startswith("DELETE FROM") and texts[1].startswith("COPY")
    assert summary.rows_deleted == 7 and summary.rows_loaded == 1
    assert conn.events[-1] == "commit"


def test_load_window_exceptions_get_their_own_file_per_window():
    conn = FakeConn()
    exceptions = pl.DataFrame({"reason": ["x"], "column": ["c"], "key": ["1"]})
    folder = Path(tempfile.mkdtemp())
    summary = load.load_window(conn, window_mapping(), context(), [result({"st_time": [datetime(2026, 1, 1)]}, exceptions)],
                               window_no=3, start=None, end=None, report_dir=folder)
    assert summary.exceptions_file == folder / "autodata_w0003_exceptions.csv"


def test_load_window_hook_runs_before_the_commit_and_rehearsal_rolls_back():
    conn = FakeConn()
    seen = []
    load.load_window(conn, window_mapping(), context(), [result({"st_time": [datetime(2026, 1, 1)]})],
                     window_no=1, start=None, end=None, report_dir=Path(tempfile.mkdtemp()),
                     before_commit=lambda c, s: seen.append(list(c.events)))
    assert seen == [["commit"]] and conn.events[-1] == "commit"

    conn2 = FakeConn()
    load.load_window(conn2, window_mapping(), context(), [result({"st_time": [datetime(2026, 1, 1)]})],
                     window_no=1, start=None, end=None, report_dir=Path(tempfile.mkdtemp()), rehearse=True,
                     before_commit=lambda c, s: seen.append("never"))
    assert conn2.events[-1] == "rollback" and "never" not in seen


def test_a_failure_rolls_back_and_raises():
    class Broken(FakeConn):
        def cursor(self):
            raise RuntimeError("disk full")

    conn = Broken()
    try:
        load.load_table(conn, mapping(), context(), [result({"rework_no": [1], "rework_id": ["a"]})],
                        report_dir=Path(tempfile.mkdtemp()), require_empty=False)
    except RuntimeError:
        assert conn.events[-1] == "rollback"
    else:
        raise AssertionError("the error must be raised")


def test_exceptions_are_written_to_a_file_and_counted():
    conn = FakeConn()
    exceptions = pl.DataFrame({"reason": ["duplicate_key"], "column": ["k"], "key": ["a"]})
    folder = Path(tempfile.mkdtemp())
    summary = load.load_table(conn, mapping(), context({"rework_information.x": 3}),
                              [result({"rework_no": [1], "rework_id": ["a"]}, exceptions)], report_dir=folder)
    assert summary.rows_with_exceptions == 1
    assert summary.exceptions_file == folder / "rework_information_exceptions.csv"
    assert "duplicate_key" in summary.exceptions_file.read_text()
    assert summary.lookup_misses == {"rework_information.x": 3}


def test_no_exceptions_means_no_file():
    assert load.write_exceptions(Path(tempfile.mkdtemp()), "t",
                                 pl.DataFrame(schema={"reason": pl.String})) is None


def test_sequence_name_is_read_from_the_default():
    assert load.sequence_name("nextval('production_core.autodata_id_seq'::regclass)") == "production_core.autodata_id_seq"
    assert load.sequence_name("false") is None and load.sequence_name(None) is None


def test_delete_window_is_half_open_and_has_a_null_window():
    conn = FakeConn()
    n = load.delete_window(conn, "production_core", "autodata", "st_time",
                           datetime(2026, 1, 1), datetime(2026, 1, 16))
    text, params = conn.statements[0]
    assert text == 'DELETE FROM "production_core"."autodata" WHERE "st_time" >= %s AND "st_time" < %s'
    assert params == (datetime(2026, 1, 1), datetime(2026, 1, 16)) and n == 7

    conn2 = FakeConn()
    load.delete_window(conn2, "production_core", "autodata", "st_time", None, None)
    assert conn2.statements[0][0] == 'DELETE FROM "production_core"."autodata" WHERE "st_time" IS NULL'


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
