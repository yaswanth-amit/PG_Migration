"""Offline tests for tracking.py and log_setup.py. A fake connection records the SQL; no database is touched.
Run with:  python test_tracking.py     (or: pytest test_tracking.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import logging
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl

from pipeline import tracking
from pipeline.tracking import Tracker
from pipeline import log_setup
class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=()):
        self.conn.statements.append((" ".join(query.split()), params))

    def fetchone(self):
        return self.conn.fetchone_value

    def fetchall(self):
        return self.conn.fetchall_value


class FakeConn:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self.statements = []
        self.fetchone_value = fetchone_value
        self.fetchall_value = fetchall_value or []

    def cursor(self):
        return FakeCursor(self)


# ---------------------------------------------------------------- tracking
def test_start_run_records_mode_tables_and_databases_and_returns_the_run_id():
    conn = FakeConn(fetchone_value=("11111111-2222-3333-4444-555555555555",))
    run_id = Tracker(conn).start_run("rehearsal", ["a", "b"], "TPM_PGSQL", "tpm_target")
    sql_text, params = conn.statements[0]
    assert sql_text.startswith("INSERT INTO migration.run") and "RETURNING run_id" in sql_text
    assert params[0] == "rehearsal" and params[2:5] == ("TPM_PGSQL", "tpm_target", ["a", "b"])
    assert run_id == "11111111-2222-3333-4444-555555555555"


def test_an_unknown_mode_is_refused_before_anything_is_sent():
    conn = FakeConn()
    try:
        Tracker(conn).start_run("production", [], "s", "t")
    except ValueError:
        assert conn.statements == []
    else:
        raise AssertionError("an unknown mode must raise")


def test_status_is_completed_or_completed_with_exceptions():
    assert tracking.status_for(0) == "completed"
    assert tracking.status_for(3) == "completed_with_exceptions"


def test_finish_table_writes_counts_status_and_lookup_misses_as_json():
    conn = FakeConn()
    Tracker(conn).finish_table("run1", "plant_machine", rows_read=2, rows_loaded=0, rows_excepted=4,
                               lookup_misses={"plant_machine.plant_iot_id": 2}, exceptions_file="x.csv")
    sql_text, params = conn.statements[0]
    assert sql_text.startswith("UPDATE migration.table_run")
    assert params[:5] == ("completed_with_exceptions", 2, 0, 4, '{"plant_machine.plant_iot_id": 2}')
    assert params[5:] == ("x.csv", "run1", "plant_machine")


def test_window_result_can_use_any_connection_so_it_can_join_the_data_transaction():
    data_conn = FakeConn()
    tracking.write_window_result(data_conn, "run1", "autodata", 3, rows_read=100, rows_loaded=100, rows_excepted=0)
    sql_text, params = data_conn.statements[0]
    assert sql_text.startswith("UPDATE migration.window_run")
    assert params == ("completed", 100, 100, 0, "run1", "autodata", 3)


def test_plan_windows_numbers_them_from_one_and_keeps_the_null_window():
    conn = FakeConn()
    windows = [(datetime(2026, 1, 1), datetime(2026, 1, 16)), (datetime(2026, 1, 16), datetime(2026, 2, 1)), (None, None)]
    Tracker(conn).plan_windows("run1", "autodata", windows)
    assert [s[1][2] for s in conn.statements] == [1, 2, 3]
    assert conn.statements[2][1][3:] == (None, None)


def test_exceptions_are_stored_as_counts_per_reason_and_column_never_as_rows():
    conn = FakeConn()
    frame = pl.DataFrame({"reason": ["dup", "dup", "null"], "column": ["k", "k", "n"], "key": ["a", "b", "c"]})
    Tracker(conn).record_exceptions("run1", "t", frame, window_no=2)
    stored = sorted((p[3], p[4], p[5]) for _, p in conn.statements)
    assert stored == [("dup", "k", 2), ("null", "n", 1)]
    assert all("a" not in p[3:5] or p[3] == "dup" for _, p in conn.statements)      # no natural keys in the parameters
    assert not any(k in str(p) for _, p in conn.statements for k in ("'b'", "'c'"))


def test_no_exceptions_writes_nothing():
    conn = FakeConn()
    Tracker(conn).record_exceptions("run1", "t", pl.DataFrame(schema={"reason": pl.String, "column": pl.String, "key": pl.String}))
    assert conn.statements == []


def test_completed_tables_and_windows_are_read_back_as_sets():
    conn = FakeConn(fetchall_value=[("customer_information",), ("down_category",)])
    assert Tracker(conn).completed_tables() == {"customer_information", "down_category"}

    start, end = datetime(2026, 1, 1), datetime(2026, 1, 16)
    conn2 = FakeConn(fetchall_value=[(start, end), (None, None)])
    assert Tracker(conn2).completed_windows("autodata") == {(start, end), (None, None)}


def test_event_levels_are_checked_and_long_messages_are_cut():
    conn = FakeConn()
    t = Tracker(conn)
    t.event("run1", "WARNING", "x" * 5000, "t")
    assert len(conn.statements[0][1][3]) == 2000
    try:
        t.event("run1", "DEBUG", "no")
    except ValueError:
        pass
    else:
        raise AssertionError("an unknown level must raise")


# ---------------------------------------------------------------- logging
def test_log_lines_carry_run_table_and_window_and_go_to_a_file():
    folder = Path(tempfile.mkdtemp())
    logger = log_setup.setup_logging("abc123", log_dir=folder)
    with log_setup.log_context(table="autodata", window="3"):
        logger.info("loaded 100 rows")
    for handler in logger.handlers:
        handler.flush()
    text = (folder / "run_abc123.log").read_text(encoding="utf-8")
    assert "run=abc123" in text and "table=autodata" in text and "window=3" in text and "loaded 100 rows" in text
    log_setup.setup_logging("abc123", log_dir=folder)           # calling again must not duplicate handlers
    assert len(logging.getLogger(log_setup.LOGGER_NAME).handlers) == 2


def test_passwords_are_masked_in_every_message():
    folder = Path(tempfile.mkdtemp())
    logger = log_setup.setup_logging("mask1", log_dir=folder)
    logger.error("connect failed: DRIVER=x;PWD={s3cret;}};UID=u password=hunter2")
    for handler in logger.handlers:
        handler.flush()
    text = (folder / "run_mask1.log").read_text(encoding="utf-8")
    assert "s3cret" not in text and "hunter2" not in text and "***" in text


def test_a_braced_password_with_escaped_braces_is_fully_masked():
    folder = Path(tempfile.mkdtemp())
    logger = log_setup.setup_logging("mask2", log_dir=folder)
    logger.error("odbc: SERVER=s;PWD={ab}}cd;ef};UID=u")
    for handler in logger.handlers:
        handler.flush()
    text = (folder / "run_mask2.log").read_text(encoding="utf-8")
    assert "ab}" not in text and "cd" not in text and "ef" not in text
    assert "PWD=***;UID=u" in text           # the text after the password survives in the FILE too


def test_log_context_is_restored_after_the_block():
    folder = Path(tempfile.mkdtemp())
    logger = log_setup.setup_logging("ctx1", log_dir=folder)
    with log_setup.log_context(table="a"):
        with log_setup.log_context(table="b"):
            pass
        logger.info("inside a")
    logger.info("outside")
    for handler in logger.handlers:
        handler.flush()
    lines = (folder / "run_ctx1.log").read_text(encoding="utf-8").splitlines()
    assert "table=a" in lines[0] and "table=-" in lines[1]


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
