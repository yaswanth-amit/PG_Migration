import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import polars as pl
from psycopg import sql

from pipeline.mapping_config import TableMapping
from pipeline.target_schema import DEFAULT_SEQUENCE
from pipeline.transform import TransformContext, TransformResult

BASE_DIR = Path(__file__).resolve().parent.parent      # the project folder
REPORT_DIR = BASE_DIR / "reports"


class TargetNotEmptyError(RuntimeError):
    """The target table already holds rows, so a plain load would mix old and new data."""


@dataclass
class LoadSummary:
    table : str
    rows_loaded : int = 0
    rows_deleted : int = 0               # window loads: rows of an earlier attempt that were removed first
    rows_with_exceptions : int = 0
    lookup_misses : dict[str, int] = field(default_factory=dict)
    seconds : float = 0.0
    rehearsal : bool = False            # True: everything was done, then rolled back
    exceptions_file : Path | None = None


def target_schema_name(m : TableMapping) -> str:
    return (m.raw.get("target") or {}).get("schema", "public")


def copy_dataframe(conn , schema : str , table : str , df : pl.DataFrame) -> int:
    """COPY the rows of a DataFrame into schema.table. Returns the number of rows written."""
    if df.height == 0:
        return 0

    query = sql.SQL("COPY {tbl} ({cols}) FROM STDIN").format(
        tbl=sql.Identifier(schema, table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in df.columns),
    )
    with conn.cursor() as cur:
        with cur.copy(query) as copy:
            for row in df.iter_rows():
                copy.write_row(row)
    return df.height


def assert_target_empty(conn , schema : str , table : str) -> None:
    query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {tbl})").format(tbl=sql.Identifier(schema, table))
    with conn.cursor() as cur:
        cur.execute(query)
        if cur.fetchone()[0]:
            raise TargetNotEmptyError(f"{schema}.{table} already contains rows; refusing to load into it.")


def sequence_name(default_expr : str | None) -> str | None:
    """'nextval('master_data.x_id_seq'::regclass)' -> 'master_data.x_id_seq'."""
    if not default_expr:
        return None
    found = re.search(r"nextval\('([^']+)'", default_expr)
    return found.group(1) if found else None


def advance_sequences(conn , schema : str , table : str , written_columns : list[str] , target_schema : dict) -> list[str]:
    """After copying source ids into a column that has a sequence default (for example rework_no),
    move the sequence to the largest value, so the next normal insert does not collide.
    Returns the names of the sequences that were advanced."""
    advanced : list[str] = []

    for name in written_columns:
        column = target_schema[name]
        if column.default_kind != DEFAULT_SEQUENCE:
            continue
        seq = sequence_name(column.default_expr)
        if seq is None:
            continue

        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT MAX({col}) FROM {tbl}").format(
                col=sql.Identifier(name), tbl=sql.Identifier(schema, table)))
            largest = cur.fetchone()[0]
            if largest is None:
                continue
            cur.execute("SELECT setval(%s::regclass, %s, true)", (seq, largest))
            advanced.append(seq)

    return advanced


def write_exceptions(report_dir : Path , table : str , exceptions : pl.DataFrame ,
                     window_no : int | None = None) -> Path | None:
    """Write the exceptions of one table (or one window) to a CSV file: reason, column and natural key only."""
    if exceptions.height == 0:
        return None
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = table if window_no is None else f"{table}_w{window_no:04d}"
    path = report_dir / f"{stem}_exceptions.csv"
    exceptions.write_csv(path)
    return path


def load_table(conn , m : TableMapping , ctx : TransformContext , results : Iterable[TransformResult] ,
               report_dir : Path = REPORT_DIR , require_empty : bool = True ,
               rehearse : bool = False , before_commit : Callable | None = None ,
               manage_transaction : bool = True) -> LoadSummary:
    """Load all batches of one table in ONE transaction: all of it is stored, or none of it.

    require_empty      - refuse to load when the target table already holds rows
    rehearse           - prove the database accepts the rows without keeping them. Sequences are not moved.
                         Note: a rolled-back load still consumes sequence values of columns with a default.
    before_commit      - optional function(conn, summary), called just before the commit of a real load.
                         Use it to record the table as completed in the SAME transaction as its rows.
                         It is not called for a rehearsal, and if it raises, the whole load is rolled back.
    manage_transaction - True: this function commits (real) or rolls back (rehearsal) by itself.
                         False: the caller owns the transaction (a rehearsal of a whole chain of tables in
                         one transaction, so a child table sees the parent rows loaded just before it).
    """
    schema = target_schema_name(m)
    table = m.tgt_table
    summary = LoadSummary(table=m.name, rehearsal=rehearse)
    started = time.monotonic()
    exception_parts : list[pl.DataFrame] = []
    written_columns : list[str] = []

    if manage_transaction:
        conn.commit()                     # end any read transaction opened earlier
    try:
        if require_empty:
            assert_target_empty(conn, schema, table)

        for result in results:
            written_columns = result.rows.columns
            summary.rows_loaded += copy_dataframe(conn, schema, table, result.rows)
            summary.rows_with_exceptions += result.exceptions.height
            if result.exceptions.height:
                exception_parts.append(result.exceptions)

        if written_columns and not rehearse:
            advance_sequences(conn, schema, table, written_columns, ctx.target_schema)

        if manage_transaction:
            if rehearse:
                conn.rollback()
            else:
                if before_commit is not None:
                    before_commit(conn, summary)
                conn.commit()
    except Exception:
        if manage_transaction:
            conn.rollback()
        raise

    summary.seconds = time.monotonic() - started
    summary.lookup_misses = {k: v for k, v in ctx.lookup_misses.items() if k.startswith(m.name + ".") and v}
    if exception_parts:
        summary.exceptions_file = write_exceptions(report_dir, m.name, pl.concat(exception_parts))
    return summary


# ----------------------------------------------------------------------------- windows (autodata)

def delete_window(conn , schema : str , table : str , time_column : str ,
                  start : datetime | None , end : datetime | None) -> int:
    """Delete the target rows of one 15-day window, so a failed or repeated window can be loaded again.
    Half-open like the extract: time_column >= start AND time_column < end.
    start = end = None means the NULL window (time_column IS NULL)."""
    if start is None and end is None:
        query = sql.SQL("DELETE FROM {tbl} WHERE {col} IS NULL")
        params : tuple = ()
    else:
        query = sql.SQL("DELETE FROM {tbl} WHERE {col} >= %s AND {col} < %s")
        params = (start, end)

    query = query.format(tbl=sql.Identifier(schema, table), col=sql.Identifier(time_column))
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


def window_has_rows(conn , m : TableMapping , start : datetime | None , end : datetime | None) -> bool:
    """Does the target hold at least one row of this window? Used to check that a window the history calls
    'completed' is really still in the target (the target may have been emptied since)."""
    col = sql.Identifier(window_target_column(m))
    tbl = sql.Identifier(target_schema_name(m), m.tgt_table)
    if start is None:
        query, params = sql.SQL("SELECT EXISTS (SELECT 1 FROM {t} WHERE {c} IS NULL)").format(t=tbl, c=col), ()
    else:
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {t} WHERE {c} >= %s AND {c} < %s)").format(t=tbl, c=col)
        params = (start, end)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return bool(cur.fetchone()[0])


def window_target_column(m : TableMapping) -> str:
    """The TARGET column that holds the window time, found from the mapping (sttime -> st_time)."""
    source_column = ((m.raw.get("batching") or {}).get("column") or "").lower()
    for col in m.columns:
        if col.src and col.src.lower() == source_column:
            return col.dst
    raise ValueError(f"Table '{m.name}': no target column is mapped from the window column '{source_column}'.")


def load_window(conn , m : TableMapping , ctx : TransformContext , results : Iterable[TransformResult] ,
                window_no : int , start : datetime | None , end : datetime | None ,
                report_dir : Path = REPORT_DIR , rehearse : bool = False ,
                before_commit : Callable | None = None , manage_transaction : bool = True) -> LoadSummary:
    """Load ONE time window in one transaction: first delete the rows of an earlier attempt at the same
    window, then insert the new rows. Safe to repeat. This relies on the target holding only migrated rows
    (confirmed for production_core.autodata)."""
    schema = target_schema_name(m)
    table = m.tgt_table
    time_column = window_target_column(m)
    summary = LoadSummary(table=m.name, rehearsal=rehearse)
    started = time.monotonic()
    exception_parts : list[pl.DataFrame] = []

    if manage_transaction:
        conn.commit()
    try:
        summary.rows_deleted = delete_window(conn, schema, table, time_column, start, end)

        for result in results:
            summary.rows_loaded += copy_dataframe(conn, schema, table, result.rows)
            summary.rows_with_exceptions += result.exceptions.height
            if result.exceptions.height:
                exception_parts.append(result.exceptions)

        if manage_transaction:
            if rehearse:
                conn.rollback()
            else:
                if before_commit is not None:
                    before_commit(conn, summary)
                conn.commit()
    except Exception:
        if manage_transaction:
            conn.rollback()
        raise

    summary.seconds = time.monotonic() - started
    summary.lookup_misses = {k: v for k, v in ctx.lookup_misses.items() if k.startswith(m.name + ".") and v}
    if exception_parts:
        summary.exceptions_file = write_exceptions(report_dir, m.name, pl.concat(exception_parts), window_no)
    return summary


if __name__ == "__main__":
    # Nothing here writes to a database. The real run belongs in a separate, reviewed script
    # that is pointed at a test database first.
    print("load.py defines the load functions; run test_load.py for the offline tests.")
