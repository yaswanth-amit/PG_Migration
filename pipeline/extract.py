import polars as pl
from datetime import datetime, timedelta
from typing import Callable, Iterator, Sequence
from pipeline.mapping_config import TableMapping

# DATEDIFF counts second boundaries crossed, so a cycle can be up to one second longer than the
# value it returns. A small safety margin keeps the lookback below from ever excluding a real match.
ICD_LOOKBACK_MARGIN_SECONDS = 2


def _add_column(name, columns : list[str], seen : set[str]) -> None:
    """Add a source column once. SQL Server column names are case-insensitive,
    so 'machineid' and 'MachineID' count as the same column."""
    if isinstance(name, str) and name and name.lower() not in seen:
        seen.add(name.lower())
        columns.append(name)


def collect_source_columns(m : TableMapping) -> list[str]:
    columns : list[str] = []
    seen : set[str] = set()

    for col in m.columns:
        _add_column(col.src, columns, seen)

        # Additional source columns named in the rule parameters
        _add_column(col.params.get("fallback_col"), columns, seen)

        order_by = col.params.get("order_by")
        if isinstance(order_by, list):
            for c in order_by:
                _add_column(c, columns, seen)
        else:
            _add_column(order_by, columns, seen)

        # Lookups match target columns to SOURCE columns; values starting with "@" refer to a
        # target column that is already resolved, so they are not source columns.
        for key in ("match", "predefined_match"):
            match = col.params.get(key)
            if isinstance(match, dict):
                for value in match.values():
                    if isinstance(value, str) and not value.startswith("@"):
                        _add_column(value, columns, seen)

        # when: {column: X, in: [...]} reads source column X
        when = col.params.get("when")
        if isinstance(when, dict):
            _add_column(when.get("column"), columns, seen)

    src_sec = m.raw.get("source") or {}
    for extra in ("group_by", "natural_key"):
        values = src_sec.get(extra)
        if isinstance(values, list):
            for c in values:
                _add_column(c, columns, seen)

    return columns


def quote_ident(name : str) -> str:
    clean = name.replace("]", "]]")
    return f"[{clean}]"


def _source_table_sql(m : TableMapping) -> str:
    src_sec = m.raw.get("source") or {}
    schema = src_sec.get("schema", "dbo")
    table = m.src_table or src_sec.get("table")
    if not table:
        raise ValueError(f"Table '{m.name}' has no source table defined.")
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def is_icd_sql(m : TableMapping , lookback_seconds : int) -> str:
    """SQL for the autodata rule 'is_icd' (see derived_rules.is_icd in autodata.yaml).

    A down row (datatype 2) is an ICD when a cycle row (datatype 1) of the same machine ran longer than its
    cycletime and contains the down row. The cycle can start in an EARLIER 15-day window, so the rule is
    evaluated against the whole table, not just the rows of the current window. The lookback
    (c.sttime >= ...) only lets SQL Server seek on the (mc, sttime) index: a containing cycle cannot start
    earlier than the longest qualifying cycle (+ margin) before the down row, so the answer never changes.
    DATEDIFF_BIG is used instead of DATEDIFF so a bad timestamp (for example year 9999) cannot overflow.
    """
    seconds = int(lookback_seconds) + ICD_LOOKBACK_MARGIN_SECONDS
    return (
        "CASE WHEN [datatype] = 2 AND EXISTS ("
        f" SELECT 1 FROM {_source_table_sql(m)} AS c"
        " WHERE c.[mc] = a.[mc] AND c.[datatype] = 1"
        "   AND c.[sttime] < a.[sttime]"
        f"   AND c.[sttime] >= DATEADD(second, -{seconds}, CASE WHEN a.[ndtime] < a.[sttime] THEN a.[ndtime] ELSE a.[sttime] END)"
        "   AND DATEDIFF_BIG(second, c.[sttime], c.[ndtime]) > c.[cycletime]"
        "   AND a.[sttime] > c.[sttime] AND a.[ndtime] < c.[ndtime])"
        " THEN 1 ELSE 0 END"
    )


# derived rule name -> function(mapping, lookback_seconds) that returns the SQL expression
DERIVED_SQL : dict[str, Callable] = {"is_icd": is_icd_sql}


def derived_columns(m : TableMapping) -> list[str]:
    return [c.params["name"] for c in m.columns if c.rule == "derived"]


def build_select_sql(m : TableMapping , where : str | None = None , ordered : bool = True ,
                     icd_lookback_seconds : int | None = None , icd_in_frame : bool = False) -> str:
    """SELECT of only the source columns the mapping needs.
    where                - optional SQL condition using ? placeholders (the 15-day windows)
    ordered              - ORDER BY the natural key, so every run reads the rows in the same order
    icd_lookback_seconds - needed when the mapping has the derived rule is_icd (see compute_icd_lookback)
    icd_in_frame         - leave is_icd out of the SQL: it is added to every batch by add_is_icd (much faster)"""
    columns = [quote_ident(c) for c in collect_source_columns(m)]

    for name in derived_columns(m):
        if icd_in_frame and name == "is_icd":
            continue
        if name not in DERIVED_SQL:
            raise ValueError(f"Table '{m.name}': no SQL is defined for the derived rule '{name}'.")
        if name == "is_icd" and icd_lookback_seconds is None:
            raise ValueError(f"Table '{m.name}': the derived rule 'is_icd' needs icd_lookback_seconds.")
        columns.append(f"{DERIVED_SQL[name](m, icd_lookback_seconds)} AS {quote_ident(name)}")

    cols_sql = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols_sql} FROM {_source_table_sql(m)} AS a"

    if where:
        sql += f" WHERE {where}"

    if ordered:
        key = (m.raw.get("source") or {}).get("natural_key")
        if isinstance(key, list) and key:
            sql += " ORDER BY " + ", ".join(quote_ident(c) for c in key)

    return sql


def extract_batches(conn , m : TableMapping , where : str | None = None , params : Sequence = () ,
                    batch_size : int = 50_000 , icd_lookback_seconds : int | None = None ,
                    icd_cycles : pl.DataFrame | None = None) -> Iterator[pl.DataFrame]:
    """icd_cycles given: is_icd is computed here in the DataFrame (see extract_all_icd_cycles / add_is_icd)
    instead of by a correlated subquery in SQL Server, which is orders of magnitude slower."""
    in_frame = icd_cycles is not None
    sql = build_select_sql(m, where=where, icd_lookback_seconds=icd_lookback_seconds, icd_in_frame=in_frame)
    cursor = conn.cursor()
    try:
        cursor.execute(sql, *params)

        col_names = [desc[0] for desc in cursor.description]
        first_schema : dict | None = None

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            # Convert pyodbc.Row to tuples, then into a Polars DataFrame.
            # infer_schema_length=None looks at every row of the batch, so a column that is NULL
            # in the first rows but has values later is still typed correctly.
            df = pl.DataFrame(
                [tuple(r) for r in rows],
                schema=col_names,
                orient="row",
                infer_schema_length=None,
            )

            # A later batch whose column is entirely NULL gets the Null type: use the type of the first batch.
            if first_schema is None:
                first_schema = dict(df.schema)
            else:
                fix = {c: t for c, t in first_schema.items()
                       if df.schema[c] == pl.Null and t != pl.Null}
                if fix:
                    df = df.cast(fix)

            if in_frame:
                df = add_is_icd(df, icd_cycles, icd_lookback_seconds or 0)
            yield df
    finally:
        cursor.close()


def count_source_rows(conn , m : TableMapping , where : str | None = None , params : Sequence = ()) -> int:
    sql = f"SELECT COUNT(*) FROM {_source_table_sql(m)}"
    if where:
        sql += f" WHERE {where}"

    cursor = conn.cursor()
    try:
        cursor.execute(sql, *params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        cursor.close()


# ----------------------------------------------------------------------------- time windows (autodata)

def generate_windows(start : datetime | None , end : datetime | None , days : int ,
                     include_null_window : bool = True) -> list[tuple[datetime | None, datetime | None]]:
    """Consecutive half-open windows [start, start + days) that cover every time from start to end.
    The first window starts at midnight of the start day. (None, None) is the extra 'time is NULL'
    window, so rows without a time are not lost."""
    if days < 1:
        raise ValueError("days must be at least 1")

    windows : list[tuple[datetime | None, datetime | None]] = []
    if start is not None and end is not None:
        first = datetime(start.year, start.month, start.day)
        step = timedelta(days=days)
        current = first
        while current <= end:               # a row exactly at 'end' falls into the window that starts at 'end'
            windows.append((current, current + step))
            current += step

    if include_null_window:
        windows.append((None, None))
    return windows


def source_time_range(conn , m : TableMapping , column : str) -> tuple[datetime | None, datetime | None]:
    """Smallest and largest value of the window column in the source (None, None for an empty table)."""
    sql = f"SELECT MIN({quote_ident(column)}), MAX({quote_ident(column)}) FROM {_source_table_sql(m)}"
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        low, high = cursor.fetchone()
        return low, high
    finally:
        cursor.close()


def window_condition(column : str , start : datetime | None , end : datetime | None) -> tuple[str, tuple]:
    """(SQL condition, parameters) for one window. Half-open: column >= start AND column < end.
    (None, None) selects the rows where the column is NULL."""
    col = quote_ident(column)
    if start is None and end is None:
        return f"{col} IS NULL", ()
    return f"{col} >= ? AND {col} < ?", (start, end)


def extract_all_icd_cycles(conn , m : TableMapping) -> pl.DataFrame:
    """Every cycle (datatype 1) that ran longer than its cycletime: the only cycles that can contain an ICD.
    There are few of them, so they are read ONCE per run and reused for every window; a query per window would
    scan the whole table each time (about 5 s on 1 crore rows)."""
    sql = (f"SELECT [mc], [sttime], [ndtime] FROM {_source_table_sql(m)} WHERE [datatype] = 1"
           " AND DATEDIFF_BIG(second, [sttime], [ndtime]) > [cycletime]")
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = [tuple(r) for r in cursor.fetchall()]
    finally:
        cursor.close()
    schema = {"mc": pl.String, "sttime": pl.Datetime, "ndtime": pl.Datetime}
    return pl.DataFrame(rows, schema=list(schema), orient="row", infer_schema_length=None) if rows else pl.DataFrame(schema=schema)


def add_is_icd(batch : pl.DataFrame , cycles : pl.DataFrame , lookback_seconds : int) -> pl.DataFrame:
    """Add the column is_icd (1 / 0), with exactly the rule of is_icd_sql: a down row (datatype 2) is an ICD when a
    cycle of the same machine, longer than its cycletime, starts before it and ends after it. A NULL time is never
    inside a cycle. Cycles never overlap in practice, but the join does not assume it: any containing cycle counts."""
    seconds = int(lookback_seconds) + ICD_LOOKBACK_MARGIN_SECONDS
    key = pl.col("mc").str.to_lowercase().str.strip_chars_end()                 # SQL Server compares text without case / trailing spaces
    down = (batch.with_row_index("_row")
            .filter((pl.col("datatype") == 2) & pl.col("sttime").is_not_null() & pl.col("ndtime").is_not_null())
            .select("_row", key.alias("_mc"), "sttime", "ndtime")
            .with_columns((pl.min_horizontal("sttime", "ndtime") - pl.duration(seconds=seconds)).alias("_low")))
    hits = [pl.DataFrame(schema={"_row": pl.UInt32})]
    if down.height and cycles.height:
        cycles = cycles.filter((pl.col("sttime") < down["sttime"].max()) & (pl.col("sttime") >= down["_low"].min()))
    if down.height and cycles.height:
        by_machine = cycles.select(key.alias("_mc"), pl.col("sttime").alias("_cst"), pl.col("ndtime").alias("_cnd"))                            .partition_by("_mc", as_dict=True)
        for (machine,), rows in down.partition_by("_mc", as_dict=True).items():
            mine = by_machine.get((machine,))
            if mine is not None:           # one machine at a time, so the join only needs the three range conditions
                hits.append(rows.join_where(mine.drop("_mc"), pl.col("_cst") < pl.col("sttime"),
                                            pl.col("ndtime") < pl.col("_cnd"), pl.col("_cst") >= pl.col("_low"))
                            .select("_row"))
    flagged = pl.concat(hits).unique()
    return (batch.with_row_index("_row")
            .with_columns(pl.col("_row").is_in(flagged["_row"].implode()).cast(pl.Int64).alias("is_icd"))
            .drop("_row"))


def compute_icd_lookback(conn , m : TableMapping) -> int:
    """Longest cycle (in seconds) that qualifies for is_icd: a datatype 1 row that ran longer than its cycletime.
    Measure it once per run on the real source; it bounds the lookback in is_icd_sql."""
    sql = (f"SELECT MAX(DATEDIFF_BIG(second, [sttime], [ndtime])) FROM {_source_table_sql(m)} "
           "WHERE [datatype] = 1 AND DATEDIFF_BIG(second, [sttime], [ndtime]) > [cycletime]")
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        value = cursor.fetchone()[0]
        return int(value) if value is not None else 0
    finally:
        cursor.close()


def extract_reference_table(conn , table : str , columns : list[str] | None = None ,
                            schema : str = "dbo") -> pl.DataFrame:
    """Read a small table in one go (for example PredefinedDownCodeInfo)."""
    cols_sql = ", ".join(quote_ident(c) for c in columns) if columns else "*"
    sql = f"SELECT {cols_sql} FROM {quote_ident(schema)}.{quote_ident(table)}"

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        col_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()

    if not rows:
        return pl.DataFrame(schema=col_names)

    return pl.DataFrame(
        [tuple(r) for r in rows],
        schema=col_names,
        orient="row",
        infer_schema_length=None,
    )


if __name__ == "__main__":
    from pipeline.mapping_config import load_mappings
    from pipeline.connections import source_connection

    ms = load_mappings()

    # Masters only: tables with date-window batching (autodata) are handled later.
    master_names = [n for n in ms.load_order if "batching" not in ms.tables[n].raw]

    mismatches = 0
    with source_connection() as conn:
        for name in master_names:
            tbl = ms.tables[name]
            expected = count_source_rows(conn, tbl)

            total = 0
            batches = 0
            for batch in extract_batches(conn, tbl):
                total += batch.height
                batches += 1

            status = "OK" if total == expected else "MISMATCH"
            if total != expected:
                mismatches += 1

            # Only counts are printed, never row values (employee_information holds passwords).
            print(f"{status:9s} {name:35s} source={expected:6d} read={total:6d} batches={batches}")

    print(f"\n{len(master_names)} tables checked, {mismatches} mismatches")
