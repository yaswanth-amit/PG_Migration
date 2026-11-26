"""Offline tests for extract.py (SQL building and window maths). No database is touched.
Run with:  python test_extract.py     (or: pytest test_extract.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

from datetime import datetime, timedelta

from pipeline.mapping_config import load_mappings
import polars as pl
from pipeline.down_aggregate import DOWN_COLUMNS
from pipeline import extract
MS = load_mappings()
T = MS.tables


# ---------------------------------------------------------------- windows
def test_windows_are_consecutive_half_open_and_end_with_the_null_window():
    w = extract.generate_windows(datetime(2026, 11, 3, 14, 30), datetime(2026, 11, 18, 17, 57), days=15)
    assert w[0] == (datetime(2026, 11, 3), datetime(2026, 11, 18))        # starts at midnight of the first day
    assert w[1] == (datetime(2026, 11, 18), datetime(2026, 12, 3))        # the next window starts where the last ended
    assert w[-1] == (None, None)
    assert len(w) == 3


def test_windows_never_overlap_and_never_leave_a_gap():
    w = extract.generate_windows(datetime(2026, 1, 1), datetime(2026, 3, 20), days=15, include_null_window=False)
    for (s1, e1), (s2, e2) in zip(w, w[1:]):
        assert e1 == s2
    assert w[0][0] <= datetime(2026, 1, 1) and w[-1][1] > datetime(2026, 3, 20)


def test_a_row_exactly_on_the_last_boundary_still_has_a_window():
    # end is exactly the start of what would be the next window: that window must exist
    w = extract.generate_windows(datetime(2026, 1, 1), datetime(2026, 1, 16), days=15, include_null_window=False)
    assert any(s <= datetime(2026, 1, 16) < e for s, e in w)


def test_one_day_range_and_empty_table():
    assert extract.generate_windows(datetime(2026, 1, 1), datetime(2026, 1, 1), days=15, include_null_window=False) \
        == [(datetime(2026, 1, 1), datetime(2026, 1, 16))]
    assert extract.generate_windows(None, None, days=15) == [(None, None)]
    assert extract.generate_windows(None, None, days=15, include_null_window=False) == []


def test_window_size_must_be_positive():
    try:
        extract.generate_windows(datetime(2026, 1, 1), datetime(2026, 1, 2), days=0)
    except ValueError:
        return
    raise AssertionError("days=0 must be refused")


def test_window_condition_is_half_open_with_parameters_and_has_a_null_form():
    sql, params = extract.window_condition("sttime", datetime(2026, 1, 1), datetime(2026, 1, 16))
    assert sql == "[sttime] >= ? AND [sttime] < ?" and params == (datetime(2026, 1, 1), datetime(2026, 1, 16))
    assert extract.window_condition("sttime", None, None) == ("[sttime] IS NULL", ())


def test_every_row_belongs_to_exactly_one_window():
    windows = extract.generate_windows(datetime(2026, 1, 1), datetime(2026, 2, 28), days=15)
    start = datetime(2026, 1, 1)
    for hours in range(0, 24 * 58 + 1, 7):               # every 7 hours across the range, boundaries included
        t = start + timedelta(hours=hours)
        hits = [1 for s, e in windows if s is not None and s <= t < e]
        assert len(hits) == 1, t


# ---------------------------------------------------------------- SQL
def test_quote_ident_escapes_a_closing_bracket():
    assert extract.quote_ident("a]b") == "[a]]b]"


def test_masters_never_select_the_biometric_columns():
    banned = {"employeeimage", "empfingerprint1", "empfingerprint2"}
    for name, m in T.items():
        selected = {c.lower() for c in extract.collect_source_columns(m)}
        assert not (banned & selected), name


def test_case_differences_do_not_select_a_column_twice():
    for m in T.values():
        columns = [c.lower() for c in extract.collect_source_columns(m)]
        assert len(columns) == len(set(columns)), m.name


def test_lookup_match_columns_are_selected_but_at_references_are_not():
    cols = extract.collect_source_columns(T["scheduled_reports"])
    assert "PlantID" in cols and "GroupID" in cols
    cols = extract.collect_source_columns(T["autodata"])
    assert "opn" in cols and not any(c.startswith("@") for c in cols)


def test_select_has_an_alias_a_quoted_names_and_an_order_by():
    sql = extract.build_select_sql(T["customer_information"])
    assert sql.startswith("SELECT [customerid]") and "FROM [dbo].[customerinformation] AS a" in sql
    assert sql.endswith("ORDER BY [customerid]")


def test_where_is_added_before_order_by():
    sql = extract.build_select_sql(T["customer_information"], where="[x] = ?")
    assert sql.index("WHERE [x] = ?") < sql.index("ORDER BY")


def test_is_icd_is_not_selected_it_is_computed_in_the_dataframe():
    sql = extract.build_select_sql(T["autodata"])
    assert "is_icd" not in sql and "EXISTS" not in sql


def test_a_table_with_is_icd_needs_the_candidate_cycles():
    try:
        next(extract.extract_batches(None, T["autodata"]))
    except ValueError as e:
        assert "prod_icd_cycles" in str(e)
    else:
        raise AssertionError("is_icd without candidate cycles must be refused")


def test_only_autodata_and_the_shift_production_table_have_derived_columns():
    for name, m in T.items():
        if name not in ("autodata", "shift_production_down_details"):
            assert extract.derived_columns(m) == [], name
    assert extract.derived_columns(T["shift_production_down_details"]) == ["cn"] + DOWN_COLUMNS


def test_the_down_columns_need_the_down_rows_of_the_window():
    try:
        next(extract.extract_batches(None, T["shift_production_down_details"]))
    except ValueError as e:
        assert "down_aggregate" in str(e)
    else:
        raise AssertionError("the down columns without down rows must be refused")


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


def test_add_cn_is_quantity_times_standard_times_over_sub_operation():
    df = pl.DataFrame({"Prod_Qty": [12.0, 117.0, 10.0, None, 4.0],
                       "CO_StdMachiningTime": [60.0, 180.0, 60.0, 60.0, 60.0],
                       "CO_StdLoadUnload": [30.0, 30.0, 30.0, 30.0, None],
                       "SubOperation": [1, 1, 2, 1, 1]})
    assert extract.add_cn(df)["cn"].to_list() == [1080.0, 24570.0, 450.0, None, None]


def test_add_cn_treats_null_or_zero_sub_operation_as_one():
    df = pl.DataFrame({"Prod_Qty": [2.0, 2.0], "CO_StdMachiningTime": [10.0, 10.0],
                       "CO_StdLoadUnload": [5.0, 5.0], "SubOperation": [None, 0]})
    assert extract.add_cn(df)["cn"].to_list() == [30.0, 30.0]


def test_add_cn_needs_its_source_columns():
    try:
        extract.add_cn(pl.DataFrame({"Prod_Qty": [1.0]}))
    except ValueError as e:
        assert "SubOperation" in str(e)
    else:
        raise AssertionError("missing source columns must be refused")


def test_a_child_table_is_read_with_a_left_join_to_its_parent():
    for name, child in (("shift_rejection_details", "ShiftRejectionDetails"), ("shift_rework_details", "ShiftReworkDetails")):
        sql = extract.build_select_sql(T[name])
        assert f"FROM [dbo].[{child}] AS a LEFT JOIN [dbo].[ShiftProductionDetails] AS p ON a.[ID] = p.[ID]" in sql
        assert "p.[pDate] AS [p.pDate]" in sql and "a.[Slno]" in sql          # parent columns are aliased, child columns qualified
        assert sql.endswith("ORDER BY a.[Slno]")


def test_the_window_condition_of_a_joined_table_uses_the_parent_column():
    assert extract.window_condition("p.pDate", datetime(2026, 1, 1), datetime(2026, 2, 1))[0] == "p.[pDate] >= ? AND p.[pDate] < ?"
    assert extract.window_condition("p.pDate", None, None)[0] == "p.[pDate] IS NULL"


def test_a_table_without_a_join_is_read_as_before():
    sql = extract.build_select_sql(T["plant_information"])
    assert " AS a" in sql and "LEFT JOIN" not in sql and "a.[" not in sql
