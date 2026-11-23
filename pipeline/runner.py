"""Runs a migration: validates the selection, then for each table in dependency order
extract -> transform -> load, with tracking and logging.

Rehearsal (the default) proves the database accepts every row and then rolls everything back. The whole
chain of selected tables runs in ONE transaction, so a child table sees the parent rows loaded just before it.
A real run commits table by table (and window by window for autodata).
"""
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from pipeline import batching
from pipeline import extract as ex
from pipeline import load as ld
from pipeline import tracking
from pipeline.log_setup import get_logger, log_context, setup_logging
from pipeline.lookups import LookupCache
from pipeline.mapping_config import MappingSet, TableMapping, table_dependencies
from pipeline.planner import plan_run, validate_selection
from pipeline.progress import NULL_PROGRESS
from pipeline.schema_adapt import adapt_to_source, list_source_columns
from pipeline.target_schema import read_target_schema, read_unique_keys
from pipeline.transform import TransformContext, TransformResult, duplicate_key_sets, transform_batch


class SelectionError(ValueError):
    """The selected tables cannot be migrated (for example a parent table is missing)."""

    def __init__(self, errors : list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass
class RunOptions:
    tables : list[str] | None = None        # None = every mapped table
    mode : str = "rehearsal"                # 'rehearsal' or 'real'
    batch_size : int = 50_000
    days : int | None = None                # window size for autodata; the mapping says 15
    start : datetime | None = None          # earliest time to include (default: the first time in the source)
    end : datetime | None = None            # latest time to include, inclusive (default: the last time in the source)
    resume : bool = True                    # real runs skip windows an earlier real run completed
    max_windows : int | None = None         # only the first N windows (a rehearsal of a big table)
    report_dir : Path = ld.REPORT_DIR
    note : str | None = None
    progress : object | None = None         # a progress.ProgressReporter; None = no live progress file


@dataclass
class TableOutcome:
    name : str
    status : str = "pending"                # completed, completed_with_exceptions, failed, blocked
    rows_read : int = 0
    rows_loaded : int = 0
    rows_excepted : int = 0
    windows_done : int = 0
    windows_total : int = 0
    seconds : float = 0.0
    error : str | None = None
    adapted : list = field(default_factory=list)   # rules left out because the source table lacks a column


@dataclass
class RunResult:
    run_id : str
    mode : str
    status : str
    outcomes : list[TableOutcome] = field(default_factory=list)


def safe_error(error : BaseException) -> str:
    """Describe an error WITHOUT quoting data. A Postgres message can contain the offending value
    (for example a COPY error quotes the column text), and employee_information holds passwords."""
    diag = getattr(error, "diag", None)
    if diag is not None:
        parts = [type(error).__name__, f"SQLSTATE {getattr(error, 'sqlstate', None)}"]
        for label, value in (("table", diag.table_name), ("column", diag.column_name),
                             ("constraint", diag.constraint_name)):
            if value:
                parts.append(f"{label}={value}")
        return " ".join(str(p) for p in parts)
    return f"{type(error).__name__}: {error}"


def _reference_tables(ms : MappingSet , names : list[str]) -> set[str]:
    """Small source tables the selected mappings read once (for example PredefinedDownCodeInfo)."""
    found : set[str] = set()
    for name in names:
        m = ms.tables[name]
        for extra in m.raw.get("extra_source_tables") or []:
            found.add(extra["table"])
        for col in m.columns:
            if col.rule == "exists_in":
                found.add(col.params["table"])
            if "predefined_names_from" in col.params:
                found.add(col.params["predefined_names_from"].split(".")[0])
    return found


class SeenKeys:
    """Duplicate check ACROSS the chunks of one load (a table, or one autodata batch). The transform already
    removes duplicates inside a chunk; this remembers the keys of the chunks before, so a row whose key was
    already loaded in this load is an exception ('duplicate_key') instead of a second copy. The first row stays."""

    def __init__(self , m : TableMapping , ctx : TransformContext | None = None):
        self.key_sets = duplicate_key_sets(m, ctx.unique_keys if ctx is not None else None)
        self.seen : dict[tuple, pl.DataFrame] = {}

    def filter(self , result : TransformResult) -> TransformResult:
        rows, extra = result.rows, []
        for keys in self.key_sets:
            if not keys or not all(k in rows.columns for k in keys) or rows.height == 0:
                continue
            numbered = rows.with_row_index("_r")
            complete = numbered.filter(pl.all_horizontal([pl.col(k).is_not_null() for k in keys]))   # NULLs never collide
            before = self.seen.get(tuple(keys))
            if before is not None and complete.height:
                dup = complete.join(before, on=keys, how="semi")
                if dup.height:
                    extra.append(dup.select(pl.lit("duplicate_key").alias("reason"), pl.lit(",".join(keys)).alias("column"),
                                            pl.concat_str([pl.col(k).cast(pl.String) for k in keys], separator="|").alias("key")))
                    rows = numbered.filter(~pl.col("_r").is_in(dup["_r"].implode())).drop("_r")
                    complete = complete.filter(~pl.col("_r").is_in(dup["_r"].implode()))
            new_keys = complete.select(keys)
            self.seen[tuple(keys)] = new_keys if before is None else pl.concat([before, new_keys])
        if not extra:
            return result
        return TransformResult(rows=rows, exceptions=pl.concat([result.exceptions] + extra))


def _results(batches , m : TableMapping , ctx : TransformContext , counters : dict , exception_frames : list ,
             progress=NULL_PROGRESS , on_batch=None):
    """Transform each batch as the loader asks for it, counting what was read.
    The generator is pulled by the loader, so the stage reported here is the real one:
    extract (fetching a batch) -> transform -> load (the loader writes it while we are suspended)."""
    seen = SeenKeys(m, ctx)
    try:
        iterator = iter(batches)
        while True:
            progress.table_stage(m.name, "extract")
            try:
                batch = next(iterator)
            except StopIteration:
                break
            counters["read"] += batch.height
            progress.table_stage(m.name, "transform")
            result = seen.filter(transform_batch(batch, m, ctx))
            if result.exceptions.height:
                exception_frames.append(result.exceptions)
            if on_batch is not None:
                on_batch()
            progress.table_stage(m.name, "load")
            yield result
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()


def _check_nothing_vanished(m : TableMapping , read : int , loaded : int , exceptions : int , where : str) -> None:
    """A row may only be missing from the target when it is listed as an exception."""
    if (m.raw.get("source") or {}).get("group_by"):
        return                                   # a grouped table has fewer rows than its source by design
    if loaded > read:
        raise ValueError(f"{where}: loaded {loaded} rows but only read {read}.")
    if read != loaded and exceptions == 0:
        raise ValueError(f"{where}: {read - loaded} rows were read but not loaded and none is listed as an exception.")


def run_migration(ms : MappingSet , options : RunOptions , src_conn , tgt_conn , tracker=None ,
                  source_db : str = "" , target_db : str = "") -> RunResult:
    if options.mode not in tracking.RUN_MODES:
        raise ValueError(f"mode must be one of {tracking.RUN_MODES}, not '{options.mode}'")

    tracker = tracker if tracker is not None else tracking.NullTracker()
    real = options.mode == "real"

    selected = set(options.tables) if options.tables else set(ms.tables)
    errors = validate_selection(selected, ms.tables, tracker.completed_tables())
    if errors:
        raise SelectionError(errors)
    order = plan_run(selected, ms.load_order)

    run_id = tracker.start_run(options.mode, order, source_db, target_db, options.note)
    setup_logging(run_id)
    log = get_logger("runner")
    log.info("run started: mode=%s tables=%d source=%s target=%s", options.mode, len(order), source_db, target_db)

    progress = options.progress if options.progress is not None else NULL_PROGRESS
    progress.start_run(run_id, options.mode, order)
    progress.phase("preparing")

    for name in order:
        tracker.plan_table(run_id, name, f"{ld.target_schema_name(ms.tables[name])}.{ms.tables[name].tgt_table}")

    outcomes : list[TableOutcome] = []
    failed : set[str] = set()
    load_time = datetime.now()

    try:
        references = {name: ex.extract_reference_table(src_conn, name) for name in _reference_tables(ms, order)}
        cache = LookupCache(tgt_conn, ms.tables)

        if options.progress is not None:          # count the source once, so the page can show a percentage
            progress.phase("counting")
            progress.set_totals(_plan_totals(ms, order, options, src_conn, tgt_conn, tracker, real))
        progress.phase("migrating")

        if not real:                              # one outer transaction for the whole rehearsal chain
            tgt_conn.commit()
            tgt_conn.execute("SELECT 1")

        for name in order:
            m = ms.tables[name]
            outcome = TableOutcome(name=name)
            outcomes.append(outcome)

            blockers = sorted(d for d in table_dependencies(m) if d in failed)
            if blockers:
                reason = f"blocked: {', '.join(blockers)} did not complete"
                outcome.status, outcome.error = "blocked", reason
                failed.add(name)
                tracker.block_table(run_id, name, reason)
                progress.table_finished(name, "blocked", error=reason)
                with log_context(table=name):
                    log.warning(reason)
                continue

            with log_context(table=name):
                started = datetime.now()
                tracker.start_table(run_id, name)
                progress.table_started(name)
                try:
                    schema_name = ld.target_schema_name(m)
                    ctx = TransformContext(
                        load_time=load_time,
                        target_schema=read_target_schema(tgt_conn, schema_name, m.tgt_table),
                        lookups=cache, references=references)

                    try:                                   # the target's own unique keys: repeats are filtered, not loaded
                        ctx.unique_keys = read_unique_keys(tgt_conn, schema_name, m.tgt_table)
                    except Exception:                      # noqa: BLE001 - without them only the YAML keys are checked
                        ctx.unique_keys = []

                    # leave out rules whose source column this source database does not have
                    m, adaptation = adapt_to_source(m, list_source_columns(src_conn, m), ctx.target_schema)
                    for note in adaptation.notes:
                        log.warning("mapping adapted: %s", note)
                    outcome.adapted = adaptation.dropped
                    if adaptation.blockers:
                        raise ValueError("; ".join(adaptation.blockers))

                    if "batching" in m.raw:
                        _run_window_table(m, ctx, outcome, options, src_conn, tgt_conn, tracker, run_id, log, real)
                    else:
                        _run_plain_table(m, ctx, outcome, options, src_conn, tgt_conn, tracker, run_id, log, real)

                    cache.forget(name)
                    outcome.status = tracking.status_for(outcome.rows_excepted)
                    progress.table_finished(name, outcome.status, outcome.rows_read, outcome.rows_loaded,
                                            outcome.rows_excepted, outcome.windows_done or None)
                    log.info("finished: read=%d loaded=%d exceptions=%d status=%s",
                             outcome.rows_read, outcome.rows_loaded, outcome.rows_excepted, outcome.status)
                except Exception as e:             # noqa: BLE001 - one table failing must not stop the independent ones
                    outcome.status, outcome.error = "failed", safe_error(e)
                    failed.add(name)
                    tracker.fail_table(run_id, name, outcome.error)
                    progress.table_finished(name, "failed", outcome.rows_read, outcome.rows_loaded,
                                            outcome.rows_excepted, outcome.windows_done or None, outcome.error)
                    log.error("failed: %s", outcome.error)
                    log.debug("stack (no values): %s", "".join(traceback.format_tb(e.__traceback__)))
                outcome.seconds = (datetime.now() - started).total_seconds()
    except KeyboardInterrupt:
        tracker.finish_run(run_id, "cancelled")
        progress.finish_run("cancelled")
        log.error("run cancelled")
        raise
    except Exception as e:                         # noqa: BLE001 - a failure outside any table must still close the run
        tracker.finish_run(run_id, "failed")
        progress.finish_run("failed")
        log.error("run failed: %s", safe_error(e))
        raise
    finally:
        if not real:
            progress.phase("finalizing")
            tgt_conn.rollback()                   # a rehearsal never keeps anything
            log.info("rehearsal rolled back: nothing was kept")

    if any(o.status in ("failed", "blocked") for o in outcomes):
        status = "failed"
    elif any(o.rows_excepted for o in outcomes):
        status = "completed_with_exceptions"
    else:
        status = "completed"
    tracker.finish_run(run_id, status)
    progress.finish_run(status)
    log.info("run finished: %s", status)
    return RunResult(run_id=run_id, mode=options.mode, status=status, outcomes=outcomes)


# ----------------------------------------------------------------------------- one table

def _run_plain_table(m , ctx , outcome , options , src_conn , tgt_conn , tracker , run_id , log , real):
    counters = {"read": 0}
    exception_frames : list[pl.DataFrame] = []
    progress = options.progress if options.progress is not None else NULL_PROGRESS
    batches = ex.extract_batches(src_conn, m, batch_size=options.batch_size)
    results = _results(batches, m, ctx, counters, exception_frames, progress,
                       on_batch=lambda: progress.table_progress(
                           m.name, counters["read"], sum(f.height for f in exception_frames)))

    def record_in_transaction(conn , summary):
        tracking.write_table_result(conn, run_id, m.name, counters["read"], summary.rows_loaded,
                                    summary.rows_with_exceptions, summary.lookup_misses or ctx_misses(m, ctx))

    if real:
        summary = ld.load_table(tgt_conn, m, ctx, results, report_dir=options.report_dir,
                                before_commit=record_in_transaction)
    else:
        with tgt_conn.transaction():              # a savepoint inside the rehearsal transaction
            summary = ld.load_table(tgt_conn, m, ctx, results, report_dir=options.report_dir,
                                    rehearse=True, manage_transaction=False)

    progress.table_stage(m.name, "record")
    _check_nothing_vanished(m, counters["read"], summary.rows_loaded, summary.rows_with_exceptions, m.name)

    outcome.rows_read, outcome.rows_loaded = counters["read"], summary.rows_loaded
    outcome.rows_excepted = summary.rows_with_exceptions
    misses = summary.lookup_misses
    for key, count in misses.items():
        log.warning("lookup misses: %s = %d", key, count)
    if exception_frames:
        frame = pl.concat(exception_frames)
        tracker.record_exceptions(run_id, m.name, frame)
        log.warning("%d rows could not be loaded: %s", frame.height, _reason_counts(frame))
    tracker.finish_table(run_id, m.name, outcome.rows_read, outcome.rows_loaded, outcome.rows_excepted,
                         misses, str(summary.exceptions_file) if summary.exceptions_file else None)


def ctx_misses(m : TableMapping , ctx : TransformContext) -> dict[str, int]:
    return {k: v for k, v in ctx.lookup_misses.items() if k.startswith(m.name + ".") and v}


def _reason_counts(frame : pl.DataFrame) -> dict[str, int]:
    return {reason: count for reason, count in frame.group_by("reason").len().iter_rows()}


def _expected_window_total(src_conn , m , column , windows) -> int:
    """How many source rows the planned windows should cover, counted in ONE query over the whole range."""
    timed = [(s, e) for s, e in windows if s is not None]
    parts, params = [], []
    if timed:
        col = ex.quote_ident(column)
        parts.append(f"({col} >= ? AND {col} < ?)")
        params += [timed[0][0], timed[-1][1]]
    if any(s is None for s, _ in windows):
        parts.append(f"{ex.quote_ident(column)} IS NULL")
    if not parts:
        return 0
    return ex.count_source_rows(src_conn, m, " OR ".join(parts), tuple(params))


def _plan_windows(m , options , src_conn , tgt_conn):
    """(windows, how, start, end, column) for a windowed table, exactly as the run will use them.
    Fixed windows of N days when --days is given or the mapping says mode: days; otherwise logical months."""
    cfg = m.raw["batching"]
    column = cfg["column"]
    low, high = ex.source_time_range(src_conn, m, column)
    start = options.start or low
    end = options.end or high
    null_window = bool(cfg.get("null_window", True))
    if options.days or cfg.get("mode", "days") != "logical_month":
        days = options.days or cfg.get("days", 15)
        windows = ex.generate_windows(start, end, days, include_null_window=null_window)
        how = f"{days} days"
    else:
        windows = batching.logical_month_windows(tgt_conn, low, high, options.start, options.end,
                                                 months=int(cfg.get("months", 1)), include_null_window=null_window)
        how = "logical months"
    if options.max_windows:
        windows = windows[:options.max_windows]
    return windows, how, start, end, column


def _trusted_done_windows(m , tracker , tgt_conn , log=None) -> set:
    """Windows an earlier real run completed AND whose rows are still in the target. History alone is not
    trusted: if the target was emptied since, those windows must be loaded again."""
    done = tracker.completed_windows(m.name)
    stale = {w for w in done if not ld.window_has_rows(tgt_conn, m, *w)}
    if stale and log is not None:
        log.warning("%d window(s) are marked completed in the history but have no rows in the target: "
                    "they will be loaded again", len(stale))
    return done - stale


def _windows_total(src_conn , m , column , windows , done : set) -> int:
    """Source rows the not-yet-done windows will read."""
    if done:
        return sum(ex.count_source_rows(src_conn, m, *ex.window_condition(column, s, e))
                   for s, e in windows if (s, e) not in done)
    return _expected_window_total(src_conn, m, column, windows)


def _estimate_window_total(src_conn , m , options) -> int:
    """A total that needs no windows: all rows of the table, or the rows inside the chosen range (the exact
    windows cannot be planned yet when they depend on a table this run has not loaded, for example shift_details).
    The exact total replaces it when the table starts."""
    column = ex.quote_ident(m.raw["batching"]["column"])
    parts, params = [], []
    if options.start:
        parts.append(f"{column} >= ?")
        params.append(datetime(options.start.year, options.start.month, options.start.day))
    if options.end:
        parts.append(f"{column} < ?")
        params.append(datetime(options.end.year, options.end.month, options.end.day) + timedelta(days=2))
    where = " AND ".join(parts) or None
    return ex.count_source_rows(src_conn, m, where, tuple(params))


def _plan_totals(ms , order , options , src_conn , tgt_conn , tracker , real) -> dict[str, int | None]:
    """How many source rows each table will read, for the progress page. A table that cannot be counted
    gets None (the page then shows its rows without a percentage). Never raises."""
    totals : dict[str, int | None] = {}
    for name in order:
        m = ms.tables[name]
        try:
            if "batching" in m.raw:
                try:
                    windows, _, _, _, column = _plan_windows(m, options, src_conn, tgt_conn)
                    done = _trusted_done_windows(m, tracker, tgt_conn) if (real and options.resume) else set()
                    totals[name] = _windows_total(src_conn, m, column, windows, done)
                except Exception:                  # noqa: BLE001 - windows not plannable yet: estimate instead
                    totals[name] = _estimate_window_total(src_conn, m, options)
            else:
                totals[name] = ex.count_source_rows(src_conn, m)
        except Exception:                          # noqa: BLE001 - a count is a nicety, not a requirement
            totals[name] = None
    return totals


def _run_window_table(m , ctx , outcome , options , src_conn , tgt_conn , tracker , run_id , log , real):
    progress = options.progress if options.progress is not None else NULL_PROGRESS
    windows, how, start, end, column = _plan_windows(m, options, src_conn, tgt_conn)
    outcome.windows_total = len(windows)
    progress.table_windows(m.name, len(windows))
    tracker.plan_windows(run_id, m.name, windows)
    log.info("windows planned: %d of %s from %s to %s", len(windows), how, start, end)

    done = _trusted_done_windows(m, tracker, tgt_conn, log) if (real and options.resume) else set()
    if options.progress is not None:               # the exact total, now that the windows are known
        try:
            progress.set_totals({m.name: _windows_total(src_conn, m, column, windows, done)})
        except Exception:                          # noqa: BLE001 - the estimate stays
            pass
    if real and not done:
        ld.assert_target_empty(tgt_conn, ld.target_schema_name(m), m.tgt_table)

    lookback = ex.compute_icd_lookback(src_conn, m) if "is_icd" in ex.derived_columns(m) else None
    if lookback is not None:
        log.info("is_icd lookback: %d seconds", lookback)

    machines = None
    if lookback is not None:
        machines = ex.list_machines(src_conn, m)                                # the cycles are read per batch
        log.info("is_icd: candidate cycles are read per batch for %d machine(s)", len(machines))

    expected_total = 0
    for number, (w_start, w_end) in enumerate(windows, start=1):
        where, params = ex.window_condition(column, w_start, w_end)
        window_label = f"{number}/{len(windows)} {w_start}..{w_end}"

        if (w_start, w_end) in done:
            outcome.windows_done += 1
            progress.table_progress(m.name, outcome.rows_read, outcome.rows_excepted, outcome.windows_done, force=True)
            log.info("window %s already completed by an earlier run: skipped", window_label)
            continue

        with log_context(window=str(number)):
            rows_source = ex.count_source_rows(src_conn, m, where, params)
            expected_total += rows_source
            tracker.start_window(run_id, m.name, number, rows_source)
            counters = {"read": 0}
            exception_frames : list[pl.DataFrame] = []
            try:
                # is_icd is computed here in the DataFrame: the same rule as in SQL, but the SQL version takes
                # minutes per window on a large table. A cycle that started in an EARLIER batch and runs into this
                # one is fetched as context (never loaded again) so the down rows of this batch can be flagged.
                icd_cycles = None
                if lookback is not None:
                    icd_cycles = ex.extract_batch_icd_cycles(src_conn, m, machines, w_start, w_end, lookback)
                batches = ex.extract_batches(src_conn, m, where, params, batch_size=options.batch_size,
                                             icd_lookback_seconds=lookback, icd_cycles=icd_cycles)
                base_read, base_exc = outcome.rows_read, outcome.rows_excepted
                results = _results(batches, m, ctx, counters, exception_frames, progress,
                                   on_batch=lambda: progress.table_progress(
                                       m.name, base_read + counters["read"],
                                       base_exc + sum(f.height for f in exception_frames), outcome.windows_done))

                def record_in_transaction(conn , summary , number=number , counters=counters):
                    tracking.write_window_result(conn, run_id, m.name, number, counters["read"],
                                                 summary.rows_loaded, summary.rows_with_exceptions)

                if real:
                    summary = ld.load_window(tgt_conn, m, ctx, results, number, w_start, w_end,
                                             report_dir=options.report_dir, before_commit=record_in_transaction)
                else:
                    with tgt_conn.transaction():
                        summary = ld.load_window(tgt_conn, m, ctx, results, number, w_start, w_end,
                                                 report_dir=options.report_dir, rehearse=True,
                                                 manage_transaction=False)

                progress.table_stage(m.name, "record")
                if counters["read"] != rows_source:
                    raise ValueError(f"window {number}: read {counters['read']} rows but the source counts {rows_source}.")
                _check_nothing_vanished(m, counters["read"], summary.rows_loaded,
                                        summary.rows_with_exceptions, f"window {number}")
            except Exception as e:                 # noqa: BLE001 - record which window failed, then stop the table
                tracker.fail_window(run_id, m.name, number, safe_error(e))
                log.error("window %s failed: %s", window_label, safe_error(e))
                raise

            outcome.rows_read += counters["read"]
            outcome.rows_loaded += summary.rows_loaded
            outcome.rows_excepted += summary.rows_with_exceptions
            outcome.windows_done += 1
            progress.table_progress(m.name, outcome.rows_read, outcome.rows_excepted, outcome.windows_done, force=True)
            if exception_frames:
                frame = pl.concat(exception_frames)
                tracker.record_exceptions(run_id, m.name, frame, number)
                log.warning("window %d: %d rows could not be loaded: %s", number, frame.height, _reason_counts(frame))
            if not real:
                # a real run records the window inside the data transaction; a rehearsal records it here
                tracker.finish_window(run_id, m.name, number, counters["read"],
                                      summary.rows_loaded, summary.rows_with_exceptions)
            log.info("window %s: read=%d loaded=%d deleted_first=%d exceptions=%d",
                     window_label, counters["read"], summary.rows_loaded, summary.rows_deleted,
                     summary.rows_with_exceptions)

    # The windows together must cover exactly the rows of the planned range.
    if not done and not options.max_windows:
        expected = _expected_window_total(src_conn, m, column, windows)
        if expected != expected_total:
            raise ValueError(f"the windows counted {expected_total} rows but the planned range holds {expected}.")
        if options.start is None and options.end is None:      # the whole table: no row may fall between the batches
            whole = ex.count_source_rows(src_conn, m)
            if whole != expected_total:
                raise ValueError(f"the batches hold {expected_total} rows but the source table has {whole}.")

    tracker.finish_table(run_id, m.name, outcome.rows_read, outcome.rows_loaded, outcome.rows_excepted,
                         ctx_misses(m, ctx), None)
