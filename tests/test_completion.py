"""The 100% check and the support package. No database, no browser.
Run with:  python -m pytest tests/test_completion.py -q
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime

import pytest

from ui import completion

NOW = 1_800_000_000.0                      # a fixed "run time"; files are given matching modification times


def table(name, status="completed", total=100, read=100, loaded=100, held=0, error=None):
    return {"name": name, "status": status, "rows_total": total, "rows_read": read, "rows_loaded": loaded,
            "rows_excepted": held, "stage": None, "windows_done": 0, "windows_total": 0, "error": error}


def snapshot(tables, status="completed", mode="real"):
    return {"run_id": "abcdef12-0000-0000-0000-000000000000", "mode": mode, "status": status, "phase": "done",
            "started_ts": NOW, "updated_ts": NOW + 60, "finished_ts": NOW + 60, "tables": tables}


CLEAN = [table("a"), table("b", total=50, read=50, loaded=50)]


# ---------------------------------------------------------------- the verdict
def test_a_clean_real_run_is_100_percent():
    r = completion.check(snapshot(CLEAN), {"mode": "real", "tables": None, "start": None, "end": None, "max_windows": 0})
    assert r["verdict"] == "complete" and r["headline"].startswith("✅ 100% migrated")
    assert "2 tables" in r["headline"] and "150 rows" in r["headline"] and not r["problems"]


def test_a_clean_rehearsal_says_nothing_was_kept_and_is_not_called_100_percent_migrated():
    r = completion.check(snapshot(CLEAN, mode="rehearsal"), None)
    assert r["verdict"] == "complete" and "Rehearsal passed" in r["headline"] and "Nothing was kept" in r["headline"]
    assert "100% migrated" not in r["headline"]


def test_rows_held_back_make_it_not_100_percent_and_name_the_table():
    tables = [table("a"), table("b", "completed_with_exceptions", 100, 100, 97, 3)]
    r = completion.check(snapshot(tables, "completed_with_exceptions"))
    assert r["verdict"] == "incomplete" and "Not 100%" in r["headline"] and "3 row(s) were held back" in r["headline"]
    assert [p["table"] for p in r["problems"]] == ["b"] and r["problems"][0]["problem"] == "3 row(s) held back"
    assert r["totals"]["rows_held_back"] == 3


def test_a_failed_table_fails_the_run_and_the_tables_after_it_are_listed_as_not_reached():
    tables = [table("a"), table("b", "failed", 10, 4, 0, 0, error="connection lost"), table("c", "pending", 5, 0, 0)]
    r = completion.check(snapshot(tables, "failed"))
    assert r["verdict"] == "failed" and "at b" in r["headline"]
    problems = {p["table"]: p["problem"] for p in r["problems"]}
    assert problems["b"] == "failed: connection lost" and "ended before this table was done" in problems["c"]


def test_a_stopped_run_is_cancelled_not_100_percent():
    r = completion.check(snapshot([table("a"), table("b", "running", 10, 2, 0)], "cancelled"))
    assert r["verdict"] == "cancelled" and "stopped" in r["headline"]


def test_a_complete_table_that_read_fewer_rows_than_the_source_counted_is_a_problem():
    r = completion.check(snapshot([table("a", total=100, read=90, loaded=90)]))
    assert r["verdict"] == "incomplete" and r["problems"][0]["problem"] == "read 90 of 100 source rows"
    assert completion.table_problem(table("a", total=None, read=7, loaded=7)) is None      # not countable: no claim
    assert completion.table_problem(table("a", total=10, read=10, loaded=8)) == "2 row(s) read but not loaded"


def test_a_grouped_table_may_have_fewer_rows_than_its_source_by_design():
    """plant_groups: 136 source rows become 7 groups (source.group_by in its mapping)."""
    t = table("plant_groups", total=136, read=136, loaded=7)
    assert completion.table_problem(t) is None                                  # taken from the real mapping
    assert completion.table_problem(t, frozenset()) == "129 row(s) read but not loaded"
    assert "plant_groups" in completion.grouped_tables()
    assert completion.check(snapshot([t]))["verdict"] == "complete"
    assert completion.table_problem(table("autodata", total=10, read=10, loaded=8)) == "2 row(s) read but not loaded"


def test_a_limited_run_is_never_called_100_percent():
    opts = {"mode": "real", "tables": None, "start": datetime(2025, 10, 11), "end": datetime(2025, 12, 11),
            "max_windows": 2}
    r = completion.check(snapshot(CLEAN), opts)
    assert r["verdict"] == "limited" and "not the whole source" in r["headline"]
    assert "date range 2025-10-11 to 2025-12-11" in r["headline"] and "first 2 windows" in r["headline"]
    only = completion.check(snapshot(CLEAN), {"tables": ["a"], "start": None, "end": None})
    assert only["verdict"] == "limited" and "only 1 selected table(s)" in only["headline"]


def test_unknown_options_are_reported_as_unknown_and_nothing_is_running_or_missing():
    assert completion.check(snapshot(CLEAN), None)["options_known"] is False
    assert completion.check(None)["verdict"] == "none"
    assert completion.check(snapshot(CLEAN, "running"))["verdict"] == "running"


# ---------------------------------------------------------------- files of the run
def write_exceptions(path, rows, mtime=NOW + 30):
    path.write_text("reason,column,key\n" + "".join(f"{r},{c},{k}\n" for r, c, k in rows), encoding="utf-8")
    os.utime(path, (mtime, mtime))


@pytest.fixture
def folders(tmp_path):
    reports, logs = tmp_path / "reports", tmp_path / "logs"
    reports.mkdir(), logs.mkdir()
    write_exceptions(reports / "b_exceptions.csv", [("duplicate_key", "id", "7"), ("duplicate_key", "id", "9")])
    write_exceptions(reports / "b_w0002_exceptions.csv", [("value_does_not_fit_type", "kwh", "55")])
    write_exceptions(reports / "a_exceptions.csv", [("duplicate_key", "id", "1")], mtime=NOW - 86_400)   # an older run
    write_exceptions(reports / "zzz_exceptions.csv", [("duplicate_key", "id", "1")])                     # not a table of this run
    (reports / "missing_masters.csv").write_text("x\n", encoding="utf-8")
    (logs / "run_abcdef12-0000-0000-0000-000000000000.log").write_text(
        "2026-11-26 12:00:00 INFO run=abcdef12 table=b window=- finished: read=100\n"
        "2026-11-26 12:00:01 WARNING run=abcdef12 table=b window=- connect PWD=*** done\n", encoding="utf-8")
    return reports, logs


def test_only_the_exception_files_of_this_run_and_its_tables_are_collected(folders):
    reports, _ = folders
    snap = snapshot([table("a"), table("b", "completed_with_exceptions", 100, 100, 97, 3)])
    found = completion.exception_files(snap, reports)
    assert [(t, p.name) for t, p in found] == [("b", "b_exceptions.csv"), ("b", "b_w0002_exceptions.csv")]


def test_reason_counts_are_complete_and_explained(folders):
    reports, _ = folders
    snap = snapshot([table("b", "completed_with_exceptions", 100, 100, 97, 3)])
    rows = completion.reason_counts(completion.exception_files(snap, reports))
    assert rows[0] == {"table": "b", "reason": "duplicate_key", "column": "id", "rows": 2, "example_key": "7",
                       "meaning": completion.REASON_HELP["duplicate_key"][0]}
    assert {r["reason"] for r in rows} == {"duplicate_key", "value_does_not_fit_type"}


# ---------------------------------------------------------------- the package
def open_zip(data):
    return zipfile.ZipFile(io.BytesIO(data))


def test_the_package_holds_a_readable_summary_the_counts_the_exceptions_and_the_log(folders):
    reports, logs = folders
    snap = snapshot([table("a"), table("b", "completed_with_exceptions", 100, 100, 97, 3)], "completed_with_exceptions")
    name, data = completion.build_package(
        snap, {"mode": "real", "tables": None, "start": None, "end": None, "max_windows": 0},
        {"source": "SRV / Kiswok", "target": "pg:5433 / tpm_migration"},
        [{"table_name": "b", "lookup_misses": {"component_information.customer_iot_id": 6}}],
        datetime(2026, 11, 26, 17, 0), reports, logs)
    assert name == "migration_support_abcdef12_20261126_1700.zip"
    z = open_zip(data)
    assert set(z.namelist()) == {"SUMMARY.md", "tables.csv", "why_rows_were_held_back.csv", "progress.json", "run.log",
                                 "exceptions/b_exceptions.csv", "exceptions/b_w0002_exceptions.csv"}
    summary = z.read("SUMMARY.md").decode("utf-8")
    assert summary.splitlines()[0] == "# Migration support report"
    for expected in ("Not 100%", "`abcdef12-0000-0000-0000-000000000000`", "REAL migration", "SRV / Kiswok",
                     "all tables, the whole source", "| b | completed_with_exceptions |", "3 row(s) held back",
                     "duplicate_key", "Decide which row wins", "Lookup misses", "component_information.customer_iot_id",
                     "Internal use only"):
        assert expected in summary, expected
    tables_csv = z.read("tables.csv").decode("utf-8-sig")
    assert "table,status,source_rows,rows_read,rows_loaded,rows_held_back,problem,lookup_misses,error" in tables_csv
    assert "customer_iot_id" in tables_csv
    assert "PWD=***" in z.read("run.log").decode("utf-8")
    assert json.loads(z.read("progress.json"))["run_id"].startswith("abcdef12")


def test_a_clean_run_gives_a_package_too_with_nothing_to_fix(folders):
    reports, logs = folders
    _, data = completion.build_package(snapshot(CLEAN), None, {}, None, datetime(2026, 11, 26), reports, logs)
    summary = open_zip(data).read("SUMMARY.md").decode("utf-8")
    assert "100% migrated" in summary and "## Tables that need attention\n\nNone." in summary
    assert "scope" not in summary.lower() or "not known" in summary             # options unknown: said so


def test_the_summary_says_so_when_the_exception_files_are_gone(tmp_path):
    snap = snapshot([table("b", "completed_with_exceptions", 100, 100, 97, 3)])
    (tmp_path / "r").mkdir()
    _, data = completion.build_package(snap, None, {}, None, datetime(2026, 11, 26), tmp_path / "r", tmp_path)
    summary = open_zip(data).read("SUMMARY.md").decode()
    assert "exception files of this run were not found" in summary and "straight after the run" in summary


def test_big_files_are_cut_but_the_reason_counts_stay_complete(folders, monkeypatch):
    reports, logs = folders
    monkeypatch.setattr(completion, "MAX_EXCEPTION_ROWS", 1)
    monkeypatch.setattr(completion, "MAX_LOG_LINES", 1)
    snap = snapshot([table("b", "completed_with_exceptions", 100, 100, 97, 3)])
    _, data = completion.build_package(snap, None, {}, None, datetime(2026, 11, 26), reports, logs)
    z = open_zip(data)
    assert len(z.read("exceptions/b_exceptions.csv").decode().splitlines()) == 2          # header + 1 row
    assert "Shortened to keep the package small" in z.read("SUMMARY.md").decode()
    assert "| b | duplicate_key | id | 2 |" in z.read("SUMMARY.md").decode()                # still the full count
    assert "duplicate_key,id,2" in z.read("why_rows_were_held_back.csv").decode("utf-8-sig").replace("\r", "")
