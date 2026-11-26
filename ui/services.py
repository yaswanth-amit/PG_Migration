"""Everything the Streamlit page needs that is not drawing: reading the migration.* tables, building the
command line of a run, starting it as a separate process, and reading logs and exception files.
Plain Python, so it is tested without a browser. No row values are ever read or shown: tracking tables hold
counts, names and times only.
"""
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import yaml

from pipeline import log_setup
from pipeline.load import REPORT_DIR

BASE_DIR = Path(__file__).resolve().parent.parent      # the project folder
DONE_STATUSES = ("completed", "completed_with_exceptions")
FINISHED = DONE_STATUSES + ("failed", "blocked")


def is_offline() -> bool:
    """PG_MIGRATION_UI_OFFLINE=1 (tests and demos): never touch a database. Read at call time on purpose."""
    return os.getenv("PG_MIGRATION_UI_OFFLINE") == "1"


# ----------------------------------------------------------------------------- database (read only)

def query(sql_text : str , params : tuple = ()) -> tuple[list[dict], str | None]:
    """Run a read-only query on the target database. Returns (rows, error message or None)."""
    if is_offline():
        return [], "offline mode: the database is not used"
    try:
        from pipeline.connections import target_connection
        import psycopg
        with target_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql_text, params)
                    names = [d.name for d in cur.description]
                    return [dict(zip(names, row)) for row in cur.fetchall()], None
            except psycopg.errors.UndefinedTable:
                conn.rollback()
                return [], "The 'migration' schema does not exist yet. Create it from sql/migration_schema.sql."
    except Exception as e:                      # noqa: BLE001 - the page must keep working without a database
        return [], f"Cannot read the target database ({type(e).__name__})."


def fetch_source_range(m) -> tuple[date | None, date | None]:
    """First and last day of the windowed table in the SOURCE (read-only), so the date boxes start on data.
    (None, None) when it cannot be read."""
    if is_offline():
        return None, None
    try:
        from pipeline import extract as ex
        from pipeline.connections import source_connection
        with source_connection() as conn:
            low, high = ex.source_time_range(conn, m, m.raw["batching"]["column"])
        return (low.date() if low else None), (high.date() if high else None)
    except Exception:                           # noqa: BLE001 - the page must keep working without the source
        return None, None


SOURCE_COLUMNS_SQL = """
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION
"""


def fetch_source_columns(m) -> tuple[list[dict], str | None]:
    """Columns of the SOURCE table (read-only): name, type (with size), nullable."""
    if is_offline():
        return [], "offline mode: the database is not used"
    try:
        from pipeline.connections import source_connection
        schema = (m.raw.get("source") or {}).get("schema", "dbo")
        with source_connection() as conn:
            cur = conn.cursor()
            cur.execute(SOURCE_COLUMNS_SQL, (schema, m.src_table))
            rows = cur.fetchall()
        out = []
        for name, dtype, length, prec, scale, nullable in rows:
            if length and length > 0:
                shown = f"{dtype}({length})"
            elif length == -1:
                shown = f"{dtype}(max)"
            elif dtype in ("decimal", "numeric") and prec:
                shown = f"{dtype}({prec},{scale})"
            else:
                shown = dtype
            out.append({"name": name, "type": dtype, "type_text": shown, "nullable": nullable == "YES"})
        return out, None
    except Exception as e:                      # noqa: BLE001 - the page must keep working without the source
        return [], f"Cannot read the source database ({type(e).__name__})."


def fetch_target_columns(m) -> tuple[dict, str | None]:
    """Columns of the TARGET table (read-only) as TargetColumn objects."""
    if is_offline():
        return {}, "offline mode: the database is not used"
    try:
        from pipeline.connections import target_connection
        from pipeline.target_schema import read_target_schema
        schema = (m.raw.get("target") or {}).get("schema", "public")
        with target_connection() as conn:
            cols = read_target_schema(conn, schema, m.tgt_table)
            conn.rollback()
        return cols, None
    except Exception as e:                      # noqa: BLE001
        return {}, f"Cannot read the target database ({type(e).__name__})."


def range_has_data(start : date , end : date , low : date | None , high : date | None) -> bool:
    """False only when the chosen days are known to lie entirely outside the days that hold data."""
    if low is None or high is None:
        return True
    return start <= high and end >= low


def fetch_status() ->tuple[dict[str, dict], str | None]:
    rows, error = query("SELECT table_name, status, rows_read, rows_loaded, rows_excepted, finished_at, run_id "
                        "FROM migration.table_status")
    return {r["table_name"]: r for r in rows}, error


def fetch_runs(limit : int = 30) -> tuple[list[dict], str | None]:
    return query("SELECT run_id::text AS run_id, started_at, finished_at, mode, status, started_by, target_db "
                 "FROM migration.run ORDER BY started_at DESC LIMIT %s", (limit,))


def fetch_table_runs(run_id : str) -> tuple[list[dict], str | None]:
    return query("SELECT table_name, status, rows_read, rows_loaded, rows_excepted, lookup_misses, "
                 "started_at, finished_at, error, exceptions_file FROM migration.table_run "
                 "WHERE run_id = %s::uuid ORDER BY id", (run_id,))


def fetch_windows(run_id : str , table_name : str) -> tuple[list[dict], str | None]:
    return query("SELECT window_no, window_start, window_end, status, rows_source, rows_read, rows_loaded, "
                 "rows_excepted, started_at, finished_at, error FROM migration.window_run "
                 "WHERE run_id = %s::uuid AND table_name = %s ORDER BY window_no", (run_id, table_name))


def fetch_events(run_id : str , limit : int = 100) -> tuple[list[dict], str | None]:
    return query("SELECT at, level, table_name, message FROM migration.event "
                 "WHERE run_id = %s::uuid ORDER BY id DESC LIMIT %s", (run_id, limit))


def running_runs() -> list[dict]:
    rows, _ = query("SELECT run_id::text AS run_id, started_at, mode, started_by FROM migration.run "
                    "WHERE status = 'running' ORDER BY started_at DESC")
    return rows


def mark_running_as_stopped() -> str | None:
    """After a run process was stopped by hand: close the rows it left in 'running'. Returns an error or None."""
    if is_offline():
        return "offline mode"
    try:
        from pipeline.connections import target_connection
        with target_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE migration.window_run SET status='failed', error='stopped by the operator', "
                            "finished_at=now() WHERE status='running'")
                cur.execute("UPDATE migration.table_run SET status='failed', error='stopped by the operator', "
                            "finished_at=now() WHERE status='running'")
                cur.execute("UPDATE migration.run SET status='cancelled', finished_at=now() WHERE status='running'")
        return None
    except Exception as e:                      # noqa: BLE001
        return f"Could not update the tracking tables ({type(e).__name__})."


# ----------------------------------------------------------------------------- connection info (no secrets)

def connection_info() -> dict:
    """Host and database names only: from the settings typed on the screen when there are any, else straight
    from connections.yaml. Passwords are never read here."""
    from pipeline import connections
    typed = connections._RUNTIME
    if typed:
        raw = {"source": {k: typed["source"].get(k) for k in ("server", "database")},
               "target": {k: typed["target"].get(k) for k in ("host", "port", "database")}}
    else:
        path = BASE_DIR / "config" / "connections.yaml"
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except OSError:
            return {}
    src, tgt = raw.get("source") or {}, raw.get("target") or {}
    return {"source": f"{src.get('server', '?')} / {src.get('database', '?')}",
            "target": f"{tgt.get('host', '?')}:{tgt.get('port', 5432)} / {tgt.get('database', '?')}",
            "target_database": tgt.get("database", "")}


# ----------------------------------------------------------------------------- starting a run

def build_run_command(tables : list[str] | None , mode : str , confirm_target : str | None = None ,
                      start : datetime | None = None , end : datetime | None = None , days : int | None = None ,
                      max_windows : int | None = None , resume : bool = True ,
                      progress_file : Path | str | None = None) -> list[str]:
    """The command that starts main.py. tables=None means all. Only values chosen on the page go in."""
    command = [sys.executable, "-u", str(BASE_DIR / "main.py"), "run",
               "--tables", "all" if not tables else ",".join(tables), "--mode", mode]
    if mode == "real" and confirm_target:
        command += ["--confirm-target", confirm_target]
    if start:
        command += ["--from", start.strftime("%Y-%m-%d")]
    if end:
        command += ["--to", end.strftime("%Y-%m-%d")]
    if days:
        command += ["--days", str(int(days))]
    if max_windows is not None:
        command += ["--max-windows", str(int(max_windows))]
    if not resume:
        command.append("--no-resume")
    if progress_file:
        command += ["--progress-file", str(progress_file)]
    return command


def run_environment(settings : dict | None) -> dict[str, str]:
    """The environment of the migration process. Connection settings typed on the screen (passwords included)
    travel here, in memory, and not in the command line, so they show in no process list, log or file."""
    from pipeline.connections import ENV_SETTINGS
    env = dict(os.environ)
    env.pop(ENV_SETTINGS, None)
    if settings:
        env[ENV_SETTINGS] = json.dumps(settings)
    return env


def launch_run(command : list[str], settings : dict | None = None) -> tuple[subprocess.Popen, Path]:
    """Start the migration as its own process, so a browser refresh or a closed tab never interrupts it.
    Its console output goes to a file, which the Monitor tab shows (this is where an early error appears)."""
    log_setup.LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = log_setup.LOG_DIR / f"launch_{datetime.now():%Y%m%d_%H%M%S}.txt"
    handle = open(out_path, "w", encoding="utf-8")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(command, cwd=BASE_DIR, stdout=handle, stderr=subprocess.STDOUT, creationflags=flags,
                               env=run_environment(settings))
    return process, out_path


# ----------------------------------------------------------------------------- live progress (file written by the run)

STALE_SECONDS = 900          # a "running" file nobody has touched for this long belongs to a dead process
RUN_PHASES = [("preparing", "Prepare"), ("counting", "Count rows"), ("migrating", "Migrate"),
              ("finalizing", "Finalize")]
TABLE_STAGES = [("extract", "Extract"), ("transform", "Transform"), ("load", "Load"), ("record", "Record")]


def new_progress_path() -> Path:
    log_setup.LOG_DIR.mkdir(parents=True, exist_ok=True)
    return log_setup.LOG_DIR / f"progress_{datetime.now():%Y%m%d_%H%M%S}.json"


def read_progress(path : Path | str | None) -> dict | None:
    """The progress snapshot, or None when the file is missing or half-written."""
    if not path:
        return None
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def latest_progress_path() -> Path | None:
    """The newest progress file, so a reloaded page can pick up a run that is still going."""
    try:
        files = sorted(log_setup.LOG_DIR.glob("progress_*.json"), key=lambda p: p.stat().st_mtime)
    except OSError:
        return None
    return files[-1] if files else None


def pid_is_running(pid : int | None) -> bool | None:
    """Is a process with this id running? None when it cannot be told."""
    if not pid:
        return None
    try:
        if os.name == "nt":
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))      # PROCESS_QUERY_LIMITED_INFORMATION
            if not handle:
                return False
            code = ctypes.c_ulong()
            ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            ctypes.windll.kernel32.CloseHandle(handle)
            return bool(ok) and code.value == 259                                      # STILL_ACTIVE
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except Exception:                                    # noqa: BLE001
        return None


def is_active(snapshot : dict | None , process_alive : bool | None = None , now : float | None = None) -> bool:
    """Is this run still going? A process handle is the truth when we have one, then the process id written in
    the progress file; only when neither can be used, how recently the file was updated."""
    if not snapshot or snapshot.get("status") not in ("running", "starting"):
        return False
    if process_alive is None:
        process_alive = pid_is_running(snapshot.get("pid"))
    if process_alive is not None:
        return process_alive
    return (now or time.time()) - snapshot.get("updated_ts", 0) < STALE_SECONDS


def summarize_progress(snapshot : dict , now : float | None = None) -> dict:
    """Totals, percentage, speed and ETA for the progress cards."""
    now = now or time.time()
    tables = snapshot.get("tables", [])
    running = snapshot.get("status") in ("running", "starting")
    end = now if running else (snapshot.get("finished_ts") or snapshot.get("updated_ts") or now)
    elapsed = max(end - snapshot.get("started_ts", end), 0.0)

    rows_read = sum(t.get("rows_read", 0) for t in tables)
    rows_loaded = sum(t.get("rows_loaded", 0) for t in tables)
    rows_excepted = sum(t.get("rows_excepted", 0) for t in tables)
    known = [t for t in tables if t.get("rows_total") is not None]
    rows_total = sum(t["rows_total"] for t in known) if len(known) == len(tables) and tables else None

    pct = None
    if rows_total:
        pct = min(rows_read / rows_total, 1.0)
    elif tables:                                    # no counts: fall back to finished tables
        pct = sum(1 for t in tables if t.get("status") in FINISHED) / len(tables)
    if not running and snapshot.get("status") in DONE_STATUSES:
        pct = 1.0

    rate = rows_read / elapsed if elapsed > 1 and rows_read else 0.0
    eta = (rows_total - rows_read) / rate if running and rate and rows_total and rows_total > rows_read else None
    active = next((t for t in tables if t.get("status") == "running"), None)
    return {"rows_read": rows_read, "rows_loaded": rows_loaded, "rows_excepted": rows_excepted,
            "rows_total": rows_total, "pct": pct or 0.0, "elapsed": elapsed, "rate": rate, "eta": eta,
            "tables_total": len(tables), "tables_done": sum(1 for t in tables if t.get("status") in DONE_STATUSES),
            "tables_failed": sum(1 for t in tables if t.get("status") in ("failed", "blocked")),
            "active_table": active["name"] if active else None,
            "active_stage": active.get("stage") if active else None,
            "phase": snapshot.get("phase")}


def fmt_int(value : int | None) -> str:
    return "—" if value is None else f"{int(value):,}"


def fmt_duration(seconds : float | None) -> str:
    if seconds is None:
        return "—"
    seconds = int(seconds)
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


# ----------------------------------------------------------------------------- files

def tail_text(path : Path , lines : int = 200) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(content[-lines:])


def tail_log(run_id : str , lines : int = 200) -> str:
    return tail_text(log_setup.log_file_for(run_id), lines)


def read_exceptions_file(path_text : str | None) -> bytes | None:
    """Exceptions CSV for download. Only files inside the reports folder can be read."""
    if not path_text:
        return None
    path = Path(path_text)
    try:
        resolved = path.resolve()
        if not resolved.is_relative_to(REPORT_DIR.resolve()) or not resolved.is_file():
            return None
        return resolved.read_bytes()
    except OSError:
        return None


# ----------------------------------------------------------------------------- display helpers

STATUS_LABELS = {
    "completed": "✅ migrated",
    "completed_with_exceptions": "⚠️ migrated, some rows held back",
    "failed": "❌ failed",
}


def status_label(row : dict | None) -> str:
    if not row:
        return "— not migrated"
    return STATUS_LABELS.get(row.get("status"), str(row.get("status")))


def mapping_table_rows(ms , status : dict[str, dict]) -> list[dict]:
    """One row per table for the selection grid, in load order."""
    from pipeline.mapping_config import table_dependencies
    rows = []
    for position, name in enumerate(ms.load_order, start=1):
        m = ms.tables[name]
        schema = (m.raw.get("target") or {}).get("schema", "")
        rows.append({
            "order": position, "name": name,
            "source": f"{(m.raw.get('source') or {}).get('schema', 'dbo')}.{m.src_table}",
            "target": f"{schema}.{m.tgt_table}",
            "needs": sorted(table_dependencies(m)),
            "status": status_label(status.get(name)),
            "rows_loaded": (status.get(name) or {}).get("rows_loaded"),
            "windowed": "batching" in m.raw,
        })
    return rows


def window_progress(windows : list[dict]) -> tuple[int, int]:
    """(finished windows, total windows) for a progress bar."""
    finished = sum(1 for w in windows if w.get("status") in DONE_STATUSES)
    return finished, len(windows)
