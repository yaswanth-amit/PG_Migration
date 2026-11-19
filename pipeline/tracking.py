"""Writes progress to the migration.* tables in the target database (see sql/migration_schema.sql).

Only counts, names, times, error messages and natural keys are written - never row values.
The Tracker uses its OWN connection in autocommit mode, so progress and failures are recorded even
when a data load is rolled back. The one exception is write_window_result / write_table_result,
which can be called with the DATA connection inside the data transaction, so a window or table is
recorded as completed in exactly the same commit as its rows.
"""
import getpass
from datetime import datetime

import polars as pl

RUN_MODES = ("rehearsal", "real")
DONE = ("completed", "completed_with_exceptions")


def status_for(rows_excepted : int) -> str:
    return "completed_with_exceptions" if rows_excepted else "completed"


def write_table_result(conn , run_id : str , table_name : str , rows_read : int , rows_loaded : int ,
                       rows_excepted : int , lookup_misses : dict[str, int] | None = None ,
                       exceptions_file : str | None = None) -> None:
    """Record that a table finished. Pass the data connection (before its commit) to make it atomic."""
    import json
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE migration.table_run
                  SET status = %s, rows_read = %s, rows_loaded = %s, rows_excepted = %s,
                      lookup_misses = %s::jsonb, exceptions_file = %s, finished_at = now(), error = NULL
                WHERE run_id = %s AND table_name = %s""",
            (status_for(rows_excepted), rows_read, rows_loaded, rows_excepted,
             json.dumps(lookup_misses or {}), exceptions_file, run_id, table_name))


def write_window_result(conn , run_id : str , table_name : str , window_no : int , rows_read : int ,
                        rows_loaded : int , rows_excepted : int) -> None:
    """Record that a window finished. Pass the data connection (before its commit) to make it atomic."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE migration.window_run
                  SET status = %s, rows_read = %s, rows_loaded = %s, rows_excepted = %s,
                      finished_at = now(), error = NULL
                WHERE run_id = %s AND table_name = %s AND window_no = %s""",
            (status_for(rows_excepted), rows_read, rows_loaded, rows_excepted, run_id, table_name, window_no))


class NullTracker:
    """Does nothing. Used when no tracking schema is available (offline runs and tests)."""

    def __init__(self):
        self.calls : list[str] = []

    def start_run(self , mode , tables , source_db , target_db , note=None) -> str:
        import uuid
        return str(uuid.uuid4())

    def completed_tables(self) -> set[str]:
        return set()

    def completed_windows(self , table_name):
        return set()

    def __getattr__(self , name):
        # every other Tracker method (plan_table, start_table, finish_table, ...) is accepted and ignored
        def ignore(*args , **kwargs):
            self.calls.append(name)
        return ignore


class Tracker:
    def __init__(self, conn):
        """conn must be a connection with autocommit switched on."""
        self.conn = conn

    # ---- runs
    def start_run(self , mode : str , tables : list[str] , source_db : str , target_db : str ,
                  note : str | None = None) -> str:
        if mode not in RUN_MODES:
            raise ValueError(f"mode must be one of {RUN_MODES}, not '{mode}'")
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO migration.run (mode, started_by, source_db, target_db, tables_requested, note)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING run_id""",
                (mode, getpass.getuser(), source_db, target_db, tables, note))
            return str(cur.fetchone()[0])

    def finish_run(self , run_id : str , status : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.run SET status = %s, finished_at = now() WHERE run_id = %s",
                        (status, run_id))

    # ---- tables
    def plan_table(self , run_id : str , table_name : str , target_table : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO migration.table_run (run_id, table_name, target_table)
                   VALUES (%s, %s, %s) ON CONFLICT (run_id, table_name) DO NOTHING""",
                (run_id, table_name, target_table))

    def start_table(self , run_id : str , table_name : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.table_run SET status = 'running', started_at = now() "
                        "WHERE run_id = %s AND table_name = %s", (run_id, table_name))

    def finish_table(self , run_id : str , table_name : str , rows_read : int , rows_loaded : int ,
                     rows_excepted : int , lookup_misses : dict[str, int] | None = None ,
                     exceptions_file : str | None = None) -> None:
        write_table_result(self.conn, run_id, table_name, rows_read, rows_loaded, rows_excepted,
                           lookup_misses, exceptions_file)

    def fail_table(self , run_id : str , table_name : str , error : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.table_run SET status = 'failed', error = %s, finished_at = now() "
                        "WHERE run_id = %s AND table_name = %s", (error[:2000], run_id, table_name))

    def block_table(self , run_id : str , table_name : str , reason : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.table_run SET status = 'blocked', error = %s "
                        "WHERE run_id = %s AND table_name = %s", (reason[:2000], run_id, table_name))

    # ---- windows (autodata)
    def plan_windows(self , run_id : str , table_name : str ,
                     windows : list[tuple[datetime | None, datetime | None]]) -> None:
        """windows are (start, end) pairs; (None, None) is the 'time is NULL' window. Numbered from 1."""
        with self.conn.cursor() as cur:
            for number, (start, end) in enumerate(windows, start=1):
                cur.execute(
                    """INSERT INTO migration.window_run (run_id, table_name, window_no, window_start, window_end)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (run_id, table_name, window_no) DO NOTHING""",
                    (run_id, table_name, number, start, end))

    def start_window(self , run_id : str , table_name : str , window_no : int , rows_source : int | None) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.window_run SET status = 'running', started_at = now(), rows_source = %s "
                        "WHERE run_id = %s AND table_name = %s AND window_no = %s",
                        (rows_source, run_id, table_name, window_no))

    def finish_window(self , run_id : str , table_name : str , window_no : int , rows_read : int ,
                      rows_loaded : int , rows_excepted : int) -> None:
        write_window_result(self.conn, run_id, table_name, window_no, rows_read, rows_loaded, rows_excepted)

    def fail_window(self , run_id : str , table_name : str , window_no : int , error : str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE migration.window_run SET status = 'failed', error = %s, finished_at = now() "
                        "WHERE run_id = %s AND table_name = %s AND window_no = %s",
                        (error[:2000], run_id, table_name, window_no))

    def completed_windows(self , table_name : str) -> set[tuple[datetime | None, datetime | None]]:
        """Windows that an earlier REAL run completed, for resuming."""
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT w.window_start, w.window_end
                     FROM migration.window_run w JOIN migration.run r USING (run_id)
                    WHERE w.table_name = %s AND r.mode = 'real'
                      AND w.status IN ('completed', 'completed_with_exceptions')""", (table_name,))
            return {(start, end) for start, end in cur.fetchall()}

    # ---- exceptions and events
    def record_exceptions(self , run_id : str , table_name : str , exceptions : pl.DataFrame ,
                          window_no : int | None = None) -> None:
        """Store COUNTS per reason and column. The frame holds natural keys only, and they are not stored here."""
        if exceptions.height == 0:
            return
        counts = exceptions.group_by(["reason", "column"]).len()
        with self.conn.cursor() as cur:
            for reason, column, count in counts.iter_rows():
                cur.execute(
                    """INSERT INTO migration.exception_summary (run_id, table_name, window_no, reason, column_name, row_count)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (run_id, table_name, window_no, reason, column, count))

    def event(self , run_id : str , level : str , message : str , table_name : str | None = None) -> None:
        if level not in ("INFO", "WARNING", "ERROR"):
            raise ValueError(f"level must be INFO, WARNING or ERROR, not '{level}'")
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO migration.event (run_id, level, table_name, message) VALUES (%s, %s, %s, %s)",
                        (run_id, level, table_name, message[:2000]))

    # ---- what the UI and the planner read
    def completed_tables(self) -> set[str]:
        """Tables whose latest real run completed (the 'migrated' set used by planner.validate_selection)."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT table_name FROM migration.table_status WHERE status = ANY(%s)", (list(DONE),))
            return {row[0] for row in cur.fetchall()}
