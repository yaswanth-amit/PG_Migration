"""Command line for the SQL Server -> PostgreSQL migration.

  python main.py list                                   show every table, its source, target and dependencies
  python main.py plan --tables all                      check a selection and show the order (no database)
  python main.py run  --tables customer_information,down_category              rehearsal (default, keeps nothing)
  python main.py run  --tables all --mode real --confirm-target <target db name>   real migration

Exit codes: 0 finished, 1 a table failed, 2 bad request (selection, options, missing confirmation),
3 the migration tracking schema is missing in the target database.
"""
import argparse
import sys
from datetime import datetime, time, timedelta

from pipeline.mapping_config import MappingSet, load_mappings
from pipeline.planner import plan_run, validate_selection, dependents
from pipeline.runner import RunOptions, SelectionError, run_migration

EXIT_OK, EXIT_FAILED, EXIT_BAD_REQUEST, EXIT_NO_TRACKING_SCHEMA = 0, 1, 2, 3
REHEARSAL_DEFAULT_WINDOWS = 2


def parse_tables(text : str | None) -> list[str] | None:
    """'all' (or nothing) means every table; otherwise a comma-separated list of table names."""
    if text is None or text.strip().lower() == "all":
        return None
    return [name.strip() for name in text.split(",") if name.strip()]


def parse_day(text : str) -> datetime:
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"'{text}' is not a date like 2026-11-03") from e


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="main.py", description="SQL Server -> PostgreSQL migration")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show every table with its source, target and dependencies")

    plan = sub.add_parser("plan", help="check a selection and show the order; no database is used")
    plan.add_argument("--tables", default="all", help="'all' or a comma-separated list of table names")
    plan.add_argument("--migrated", default="", help="tables already migrated earlier (comma-separated)")

    run = sub.add_parser("run", help="migrate the selected tables")
    run.add_argument("--tables", default="all", help="'all' or a comma-separated list of table names")
    run.add_argument("--mode", choices=("rehearsal", "real"), default="rehearsal",
                     help="rehearsal (default) rolls everything back; real keeps the data")
    run.add_argument("--confirm-target", default=None,
                     help="a real run needs the exact name of the target database")
    run.add_argument("--from", dest="start", type=parse_day, default=None, help="autodata: first day to include")
    run.add_argument("--to", dest="end", type=parse_day, default=None, help="autodata: last day to include")
    run.add_argument("--days", type=int, default=None, help="autodata: days per window (mapping default: 15)")
    run.add_argument("--max-windows", type=int, default=None,
                     help="autodata: only the first N windows (0 = all; rehearsal default 2)")
    run.add_argument("--no-resume", action="store_true", help="real run: do not skip windows completed earlier")
    run.add_argument("--batch-size", type=int, default=50_000)
    run.add_argument("--progress-file", default=None,
                     help="write live progress (names, counts, stages) to this JSON file; the web page reads it")
    run.add_argument("--no-tracking", action="store_true",
                     help="rehearsal only: do not write to the migration schema")
    return parser


def options_from_args(args : argparse.Namespace) -> RunOptions:
    if args.days is not None and args.days < 1:
        raise ValueError("--days must be at least 1")
    if args.start and args.end and args.end < args.start:
        raise ValueError("--to must not be before --from")

    if args.max_windows is None:
        max_windows = REHEARSAL_DEFAULT_WINDOWS if args.mode == "rehearsal" else None
    else:
        max_windows = args.max_windows or None            # 0 = all windows

    end = None
    if args.end is not None:                              # --to is a day: include all of it
        end = datetime.combine(args.end.date(), time.max)

    progress = None
    if getattr(args, "progress_file", None):
        from pipeline.progress import ProgressReporter
        progress = ProgressReporter(args.progress_file)

    return RunOptions(tables=parse_tables(args.tables), mode=args.mode, batch_size=args.batch_size,
                      days=args.days, start=args.start, end=end, resume=not args.no_resume,
                      max_windows=max_windows, progress=progress)


# ----------------------------------------------------------------------------- commands

def cmd_list(ms : MappingSet) -> int:
    print(f"{'#':>2} {'table':32s} {'source':32s} {'target':45s} needs")
    for position, name in enumerate(ms.load_order, start=1):
        m = ms.tables[name]
        schema = (m.raw.get("target") or {}).get("schema", "")
        needs = ", ".join(sorted(set(m.depends_on) | {c.params["table"] for c in m.columns if c.rule == "lookup"} - {name}))
        print(f"{position:2d} {name:32s} {m.src_table:32s} {schema + '.' + m.tgt_table:45s} {needs}")
    return EXIT_OK


def cmd_plan(ms : MappingSet , tables : list[str] | None , migrated : set[str]) -> int:
    selected = set(tables) if tables else set(ms.tables)
    errors = validate_selection(selected, ms.tables, migrated)
    if errors:
        for error in errors:
            print("ERROR:", error)
        return EXIT_BAD_REQUEST
    for position, name in enumerate(plan_run(selected, ms.load_order), start=1):
        unlocks = dependents(name, ms.tables)
        print(f"{position:2d}. {name}" + (f"   (needed by {', '.join(unlocks)})" if unlocks else ""))
    return EXIT_OK


def cmd_run(ms : MappingSet , args : argparse.Namespace) -> int:
    options = options_from_args(args)

    # Cheap checks first: nothing below opens a connection until the request is known to be valid.
    selected = set(options.tables) if options.tables else set(ms.tables)
    if args.mode == "real" and args.no_tracking:
        print("ERROR: a real run must be tracked; --no-tracking is for rehearsals only.")
        return EXIT_BAD_REQUEST

    from pipeline.connections import load_settings, source_connection, target_connection
    settings = load_settings()
    target_db = settings["target"]["database"]
    source_db = settings["source"]["database"]

    if args.mode == "real" and args.confirm_target != target_db:
        print(f"ERROR: a real run changes the database '{target_db}'. "
              f"Repeat the command with --confirm-target {target_db}")
        return EXIT_BAD_REQUEST

    import psycopg
    from pipeline.tracking import NullTracker, Tracker

    with source_connection() as src, target_connection() as tgt, target_connection() as tracking_conn:
        if args.no_tracking:
            tracker = NullTracker()
        else:
            tracking_conn.autocommit = True
            tracker = Tracker(tracking_conn)
            try:
                tracker.completed_tables()
            except psycopg.errors.UndefinedTable:
                print("ERROR: the 'migration' schema is missing in the target database. "
                      "Create it from sql/migration_schema.sql (after review) first.")
                return EXIT_NO_TRACKING_SCHEMA

        try:
            result = run_migration(ms, options, src, tgt, tracker, source_db=source_db, target_db=target_db)
        except SelectionError as e:
            for error in e.errors:
                print("ERROR:", error)
            return EXIT_BAD_REQUEST

    print_summary(result)
    return EXIT_OK if result.status != "failed" else EXIT_FAILED


def print_summary(result) -> None:
    """Counts and names only; never row values."""
    print(f"\nrun {result.run_id}  mode={result.mode}  status={result.status}")
    print(f"{'table':32s} {'status':26s} {'read':>9s} {'loaded':>9s} {'exceptions':>10s}  note")
    for o in result.outcomes:
        note = o.error or (f"{o.windows_done}/{o.windows_total} windows" if o.windows_total else "")
        print(f"{o.name:32s} {o.status:26s} {o.rows_read:9d} {o.rows_loaded:9d} {o.rows_excepted:10d}  {note}")
    if result.mode == "rehearsal":
        print("\nRehearsal: everything was rolled back, nothing was kept in the target.")


def main(argv : list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    ms = load_mappings()

    if args.command == "list":
        return cmd_list(ms)
    if args.command == "plan":
        migrated = {n.strip() for n in args.migrated.split(",") if n.strip()}
        return cmd_plan(ms, parse_tables(args.tables), migrated)
    try:
        return cmd_run(ms, args)
    except ValueError as e:
        print("ERROR:", e)
        return EXIT_BAD_REQUEST


if __name__ == "__main__":
    sys.exit(main())
