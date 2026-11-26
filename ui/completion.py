"""After a run: is it 100% migrated? If not, what exactly is missing, and a support package for the team.

Plain Python (no Streamlit), so it is tested without a browser. Two jobs:

* check(): turns the run's progress snapshot into ONE verdict the field engineer can read, plus the list
  of tables that fell short and why. "100%" means: the run completed, every table is migrated, nothing was
  held back, and the rows read match the rows counted in the source. A run that was limited on purpose
  (a date range, the first N windows, a table subset) is never called 100%.
* build_package(): one .zip the engineer sends to the team: SUMMARY.md (read this first), tables.csv,
  why_rows_were_held_back.csv, the exception files, the run log and the raw progress file.

What the package contains: table names, counts, reasons, record keys (ids) and the run log. The log masks
passwords and never holds row values; the exception files hold the reason, the column and the record key
only. It holds NO password and NO connection details beyond the server and database names. It is still
internal company data: send it to the migration team only.
"""
import csv
import io
import json
import re
import zipfile
from collections import Counter
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from pipeline import log_setup
from pipeline.load import REPORT_DIR

RUNNING = ("running", "starting")
MAX_EXCEPTION_ROWS = 20_000            # per exception file in the package (the reason counts are always complete)
MAX_LOG_LINES = 20_000

# What each reason means and what the team usually does about it.
REASON_HELP = {
    "duplicate_key": ("Two source rows have the same key.",
                      "Decide which row wins, or fix the key in the source master."),
    "value_does_not_fit_type": ("A value is the wrong type or too big for the target column.",
                                "Widen the target column, or add a cast / cleaning rule in the mapping."),
    "null_in_not_null_column": ("The target column must have a value but the source is empty.",
                                "Give the column a default or a mapping rule."),
    "dropped_no_parent": ("The master row this record points to was not found in the target.",
                          "Add the missing master record, or correct the code in the source."),
}


def _n(value) -> int:
    return int(value or 0)


# ----------------------------------------------------------------------------- the verdict
def _limits(options : dict | None) -> list[str]:
    """What made this run smaller than the whole source (empty: nothing, or unknown when options is None)."""
    if not options:
        return []
    limits = []
    if options.get("start") or options.get("end"):
        start, end = options.get("start"), options.get("end")
        limits.append(f"date range {start:%Y-%m-%d} to {end:%Y-%m-%d}" if start and end
                      else f"date range from {start:%Y-%m-%d}" if start else f"date range up to {end:%Y-%m-%d}")
    if options.get("max_windows"):
        limits.append(f"only the first {int(options['max_windows'])} windows of the large tables")
    if options.get("tables"):
        limits.append(f"only {len(options['tables'])} selected table(s)")
    return limits


@lru_cache(maxsize=1)
def grouped_tables() -> frozenset:
    """Tables whose target has fewer rows than the source BY DESIGN (the mapping has source.group_by), for
    example plant_groups. The runner's own check allows that too, so this check must not call it a problem."""
    try:
        from pipeline.mapping_config import load_mappings
        return frozenset(n for n, m in load_mappings().tables.items() if (m.raw.get("source") or {}).get("group_by"))
    except Exception:                                   # noqa: BLE001 - without the mappings, flag more rather than less
        return frozenset()


def table_problem(t : dict, grouped : frozenset | None = None) -> str | None:
    """Why a table is not fully migrated, in plain words, or None when it is."""
    grouped = grouped_tables() if grouped is None else grouped
    status, read, loaded, held = t.get("status"), _n(t.get("rows_read")), _n(t.get("rows_loaded")), _n(t.get("rows_excepted"))
    total = t.get("rows_total")
    if status == "failed":
        return f"failed: {t.get('error') or 'see run.log'}"
    if status in (None, "pending", "running"):
        return "not finished: the run ended before this table was done"
    if held:
        return f"{held:,} row(s) held back"
    if total is not None and read < _n(total):
        return f"read {read:,} of {_n(total):,} source rows"
    if loaded < read and t.get("name") not in grouped:
        return f"{read - loaded:,} row(s) read but not loaded"
    return None


def check(snapshot : dict | None, options : dict | None = None, grouped : frozenset | None = None) -> dict:
    """The verdict for a run. `options` are the choices made when it was started (None when unknown, for
    example after the page was reloaded). verdict is one of: none, running, complete, limited, incomplete,
    failed, cancelled."""
    if not snapshot:
        return {"verdict": "none", "headline": "No run yet.", "problems": [], "tables": [], "totals": {}, "limits": []}
    mode = snapshot.get("mode", "rehearsal")
    tables = []
    for t in snapshot.get("tables", []):
        tables.append({"table": t["name"], "status": t.get("status"), "source_rows": t.get("rows_total"),
                       "rows_read": _n(t.get("rows_read")), "rows_loaded": _n(t.get("rows_loaded")),
                       "rows_held_back": _n(t.get("rows_excepted")), "problem": table_problem(t, grouped),
                       "error": t.get("error")})
    problems = [t for t in tables if t["problem"]]
    totals = {"tables": len(tables), "source_rows": sum(_n(t["source_rows"]) for t in tables),
              "rows_read": sum(t["rows_read"] for t in tables), "rows_loaded": sum(t["rows_loaded"] for t in tables),
              "rows_held_back": sum(t["rows_held_back"] for t in tables)}
    limits = _limits(options)
    status = snapshot.get("status")
    scope = "would load" if mode != "real" else "loaded"
    result = {"mode": mode, "run_status": status, "problems": problems, "tables": tables, "totals": totals,
              "limits": limits, "options_known": options is not None}

    if status in RUNNING:
        verdict, headline = "running", "The run is still going."
    elif status == "cancelled":
        verdict, headline = "cancelled", "⛔ The run was stopped before it finished."
    elif status == "failed":
        first = next((p for p in problems if (p["status"] == "failed")), None)
        verdict = "failed"
        headline = "❌ The run failed" + (f" at {first['table']}." if first else ".")
    elif problems:
        held, bad = totals["rows_held_back"], len(problems)
        verdict = "incomplete"
        headline = (f"⚠️ Not 100%: {held:,} row(s) were held back, {bad} table(s) need attention." if held
                    else f"⚠️ Not 100%: {bad} table(s) need attention.")
    elif limits:
        verdict = "limited"
        headline = ("✅ Everything in the chosen scope " + ("would load" if mode != "real" else "was migrated")
                    + f", but the run was limited ({'; '.join(limits)}). This is not the whole source.")
    elif mode != "real":
        verdict = "complete"
        headline = (f"✅ Rehearsal passed: all {totals['tables']} tables, {totals['rows_loaded']:,} rows {scope}, "
                    f"nothing held back. Nothing was kept. The real migration is next.")
    else:
        verdict = "complete"
        headline = (f"✅ 100% migrated: all {totals['tables']} tables, {totals['rows_loaded']:,} rows {scope}, "
                    f"nothing held back.")
    result.update(verdict=verdict, headline=headline)
    return result


# ----------------------------------------------------------------------------- the files of a run
_EXC_NAME = re.compile(r"^(?P<table>.+?)(?:_w\d{4})?_exceptions\.csv$")


def exception_files(snapshot : dict, report_dir : Path = REPORT_DIR, now : float | None = None) -> list[tuple[str, Path]]:
    """(table, file) for the exception files this run wrote: named after one of its tables and modified
    while it ran. (The folder also holds files of earlier runs.)"""
    if not snapshot or not report_dir.is_dir():
        return []
    start = (snapshot.get("started_ts") or 0) - 120
    end = (snapshot.get("finished_ts") or snapshot.get("updated_ts") or now or datetime.now().timestamp()) + 300
    names = {t["name"] for t in snapshot.get("tables", [])}
    found = []
    for path in sorted(report_dir.glob("*_exceptions.csv")):
        match = _EXC_NAME.match(path.name)
        try:
            fresh = start <= path.stat().st_mtime <= end
        except OSError:
            continue
        if match and match["table"] in names and fresh:
            found.append((match["table"], path))
    return found


def reason_counts(files : list[tuple[str, Path]]) -> list[dict]:
    """Why rows were held back: one row per (table, reason, column) with the count and one example key.
    Always complete, whatever the size of the exception files."""
    counts : Counter = Counter()
    example : dict = {}
    for table, path in files:
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    key = (table, row.get("reason", ""), row.get("column", ""))
                    counts[key] += 1
                    example.setdefault(key, row.get("key", ""))
        except OSError:
            continue
    return [{"table": t, "reason": r, "column": c, "rows": n, "example_key": example[(t, r, c)],
             "meaning": REASON_HELP.get(r, ("", ""))[0]}
            for (t, r, c), n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def _tail_lines(path : Path, limit : int) -> tuple[str, bool]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "", False
    return "\n".join(lines[-limit:]), len(lines) > limit


def _csv_bytes(rows : list[dict], columns : list[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")            # BOM: Excel opens it with the right characters


# ----------------------------------------------------------------------------- the support package
def _fmt_ts(ts : float | None) -> str:
    return "—" if not ts else f"{datetime.fromtimestamp(ts):%Y-%m-%d %H:%M:%S}"


def render_summary(snapshot : dict, report : dict, options : dict | None, info : dict, reasons : list[dict],
                   table_runs : list[dict], truncated : list[str], now : datetime) -> str:
    """SUMMARY.md: the first thing the team reads."""
    mode = report["mode"]
    t = report["totals"]
    lines = [f"# Migration support report", "",
             f"**Result:** {report['headline']}", "",
             f"Created {now:%Y-%m-%d %H:%M}. Internal use only: send it to the migration team and nobody else.", "",
             "## The run", "",
             f"| | |", f"|---|---|",
             f"| Run id | `{snapshot.get('run_id', '—')}` |",
             f"| Mode | {'REAL migration' if mode == 'real' else 'Rehearsal (nothing kept)'} |",
             f"| Status | {report['run_status']} |",
             f"| Started | {_fmt_ts(snapshot.get('started_ts'))} |",
             f"| Finished | {_fmt_ts(snapshot.get('finished_ts'))} |",
             f"| Source | {info.get('source', '—')} |",
             f"| Target | {info.get('target', '—')} |"]
    if options:
        limits = report["limits"]
        lines.append(f"| Scope | {'; '.join(limits) if limits else 'all tables, the whole source'} |")
    else:
        lines.append("| Scope | not known (the page was reloaded after the start): check the dates with the engineer |")
    lines += ["", "## Totals", "",
              f"* Tables: {t['tables']}",
              f"* Source rows counted: {t['source_rows']:,}",
              f"* Rows read: {t['rows_read']:,}",
              f"* Rows {'that would load' if mode != 'real' else 'loaded'}: {t['rows_loaded']:,}",
              f"* Rows held back: {t['rows_held_back']:,}", ""]

    if report["problems"]:
        lines += ["## Tables that need attention", "",
                  "| Table | Status | Source rows | Read | Loaded | Held back | Problem |", "|---|---|---:|---:|---:|---:|---|"]
        for p in report["problems"]:
            src = "—" if p["source_rows"] is None else f"{_n(p['source_rows']):,}"
            lines.append(f"| {p['table']} | {p['status']} | {src} | {p['rows_read']:,} | {p['rows_loaded']:,} | "
                         f"{p['rows_held_back']:,} | {p['problem']} |")
        lines.append("")
    else:
        lines += ["## Tables that need attention", "", "None.", ""]

    if not reasons and t["rows_held_back"]:
        lines += ["## Why rows were held back", "",
                  "The exception files of this run were not found (a later run replaced them, or they were deleted), "
                  "so the reasons are not in this package. Build the package straight after the run.", ""]
    if reasons:
        lines += ["## Why rows were held back", "",
                  "| Table | Reason | Column | Rows | Example key |", "|---|---|---|---:|---|"]
        lines += [f"| {r['table']} | {r['reason']} | {r['column']} | {r['rows']:,} | {r['example_key']} |" for r in reasons]
        lines += ["", "What the reasons mean, and the usual fix:", ""]
        for reason in sorted({r["reason"] for r in reasons}):
            meaning, fix = REASON_HELP.get(reason, ("(unknown reason)", "Look at the run log."))
            lines.append(f"* **{reason}**: {meaning} {fix}")
        lines.append("")

    misses = [(r["table_name"], r.get("lookup_misses")) for r in table_runs if r.get("lookup_misses")]
    if misses:
        lines += ["## Lookup misses (a code that was not found in a master)", ""]
        for name, value in misses:
            lines.append(f"* {name}: {json.dumps(value, default=str)}")
        lines += ["", "These need a missing master record added, or the code corrected in the source.", ""]

    errors = [p for p in report["problems"] if p["status"] == "failed" and p.get("error")]
    if errors:
        lines += ["## Errors", ""] + [f"* {p['table']}: {p['error']}" for p in errors] + [""]

    lines += ["## Files in this package", "",
              "* `SUMMARY.md`: this file",
              "* `tables.csv`: every table with its counts",
              "* `why_rows_were_held_back.csv`: the reasons, counted (complete)",
              "* `exceptions/`: the exception files (reason, column, record key)",
              "* `run.log`: the run log (passwords masked, no row values)",
              "* `progress.json`: the raw progress file"]
    if truncated:
        lines += ["", "**Shortened to keep the package small:** " + ", ".join(truncated)
                  + f". The reason counts above are complete. Full files are in the `reports` and `logs` folders of the PC."]
    return "\n".join(lines) + "\n"


def build_package(snapshot : dict, options : dict | None = None, info : dict | None = None,
                  table_runs : list[dict] | None = None, now : datetime | None = None,
                  report_dir : Path = REPORT_DIR, log_dir : Path = log_setup.LOG_DIR) -> tuple[str, bytes]:
    """(file name, zip bytes) for the team. Works with or without a database: table_runs (the tracking
    rows, with the lookup misses) is optional."""
    now = now or datetime.now()
    info, table_runs = info or {}, table_runs or []
    report = check(snapshot, options)
    files = exception_files(snapshot, report_dir)
    reasons = reason_counts(files)
    misses = {r["table_name"]: r.get("lookup_misses") for r in table_runs}
    truncated : list[str] = []

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        rows = [dict(t, lookup_misses=json.dumps(misses[t["table"]], default=str) if misses.get(t["table"]) else "")
                for t in report["tables"]]
        z.writestr("tables.csv", _csv_bytes(rows, ["table", "status", "source_rows", "rows_read", "rows_loaded",
                                                   "rows_held_back", "problem", "lookup_misses", "error"]))
        z.writestr("why_rows_were_held_back.csv",
                   _csv_bytes(reasons, ["table", "reason", "column", "rows", "example_key", "meaning"]))
        for table, path in files:
            try:
                with path.open(encoding="utf-8", newline="") as handle:
                    head = [line for _, line in zip(range(MAX_EXCEPTION_ROWS + 1), handle)]
            except OSError:
                continue
            if len(head) > MAX_EXCEPTION_ROWS:             # header + MAX rows
                head = head[:MAX_EXCEPTION_ROWS + 1]
                truncated.append(path.name)
            z.writestr(f"exceptions/{path.name}", "".join(head))
        run_id = snapshot.get("run_id")
        if run_id:
            text, cut = _tail_lines(log_setup.log_file_for(run_id, log_dir), MAX_LOG_LINES)
            if text:
                z.writestr("run.log", text + "\n")
                if cut:
                    truncated.append("run.log (last lines only)")
        z.writestr("progress.json", json.dumps(snapshot, indent=2, default=str))
        z.writestr("SUMMARY.md", render_summary(snapshot, report, options, info, reasons, table_runs, truncated, now))
    short = str(snapshot.get("run_id", "run"))[:8]
    return f"migration_support_{short}_{now:%Y%m%d_%H%M}.zip", buffer.getvalue()
