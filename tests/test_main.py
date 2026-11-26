"""Offline tests for main.py (argument handling and the safety checks). No database is touched.
Run with:  python test_main.py     (or: pytest test_main.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import contextlib
import io
from datetime import datetime, time
from unittest import mock

from pipeline import connections
import main
from main import EXIT_BAD_REQUEST, EXIT_OK


def run_cli(argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = main.main(argv)
    return code, out.getvalue()


def parse(argv):
    return main.build_parser().parse_args(argv)


# ---------------------------------------------------------------- parsing
def test_tables_all_means_every_table_and_a_list_is_split():
    assert main.parse_tables("all") is None and main.parse_tables(None) is None
    assert main.parse_tables(" a , b ,") == ["a", "b"]


def test_dates_are_parsed_and_bad_dates_are_refused():
    assert main.parse_day("2026-11-03") == datetime(2026, 11, 3)
    try:
        main.parse_day("03/11/2026")
    except Exception as e:                       # argparse.ArgumentTypeError
        assert "not a date" in str(e)
    else:
        raise AssertionError("a bad date must be refused")


def test_rehearsal_defaults_to_two_windows_and_real_to_all():
    assert main.options_from_args(parse(["run"])).max_windows == 2
    assert main.options_from_args(parse(["run", "--mode", "real"])).max_windows is None
    assert main.options_from_args(parse(["run", "--max-windows", "0"])).max_windows is None
    assert main.options_from_args(parse(["run", "--max-windows", "5"])).max_windows == 5


def test_the_to_date_includes_the_whole_day():
    o = main.options_from_args(parse(["run", "--from", "2026-11-03", "--to", "2026-11-18"]))
    assert o.start == datetime(2026, 11, 3) and o.end == datetime.combine(datetime(2026, 11, 18).date(), time.max)


def test_bad_option_values_are_refused():
    for argv in (["run", "--days", "0"], ["run", "--from", "2026-11-18", "--to", "2026-11-03"]):
        try:
            main.options_from_args(parse(argv))
        except ValueError:
            continue
        raise AssertionError(f"{argv} must be refused")


def test_the_default_mode_is_a_rehearsal_and_resume_is_on():
    o = main.options_from_args(parse(["run"]))
    assert o.mode == "rehearsal" and o.resume is True and o.tables is None


# ---------------------------------------------------------------- list and plan (no database)
def test_list_shows_every_table_in_load_order():
    code, text = run_cli(["list"])
    assert code == EXIT_OK
    lines = [l for l in text.splitlines()[1:] if l.strip()]
    assert len(lines) == 30 and lines[-1].split()[1] == "shift_rework_details"
    assert any("component_operation_pricing" in l and "machine_information" in l for l in lines)


def test_plan_refuses_cop_without_its_parents_and_accepts_it_when_they_are_migrated():
    code, text = run_cli(["plan", "--tables", "component_operation_pricing"])
    assert code == EXIT_BAD_REQUEST and "machine_information" in text and "component_information" in text

    code, text = run_cli(["plan", "--tables", "component_operation_pricing",
                          "--migrated", "machine_information,component_information"])
    assert code == EXIT_OK and "component_operation_pricing" in text


def test_plan_all_lists_28_tables_in_order():
    code, text = run_cli(["plan"])
    assert code == EXIT_OK and len(text.strip().splitlines()) == 30
    assert text.index("customer_information") < text.index("component_information") < text.index("autodata")


# ---------------------------------------------------------------- safety of `run`
def settings():
    return {"source": {"database": "TPM_PGSQL"}, "target": {"database": "tpm_target"}}


def test_a_real_run_without_the_target_name_is_refused_before_any_connection():
    with mock.patch.object(connections, "load_settings", settings), \
         mock.patch.object(connections, "source_connection", side_effect=AssertionError("must not connect")), \
         mock.patch.object(connections, "target_connection", side_effect=AssertionError("must not connect")):
        code, text = run_cli(["run", "--tables", "customer_information", "--mode", "real"])
    assert code == EXIT_BAD_REQUEST and "--confirm-target tpm_target" in text


def test_a_real_run_with_the_wrong_target_name_is_refused():
    with mock.patch.object(connections, "load_settings", settings), \
         mock.patch.object(connections, "source_connection", side_effect=AssertionError("must not connect")):
        code, text = run_cli(["run", "--tables", "customer_information", "--mode", "real",
                              "--confirm-target", "something_else"])
    assert code == EXIT_BAD_REQUEST


def test_a_real_run_cannot_switch_tracking_off():
    code, text = run_cli(["run", "--tables", "customer_information", "--mode", "real", "--no-tracking"])
    assert code == EXIT_BAD_REQUEST and "must be tracked" in text


def test_a_bad_option_gives_exit_2_not_a_traceback():
    code, text = run_cli(["run", "--days", "0"])
    assert code == EXIT_BAD_REQUEST and "ERROR" in text


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
