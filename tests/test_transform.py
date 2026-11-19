"""Unit tests for transform.py: one tiny frame per rule, no database needed.
Run with:  python test_transform.py     (or: pytest test_transform.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

from datetime import datetime
from decimal import Decimal

import polars as pl

from pipeline.mapping_config import RULES, TableMapping, ColumnRule
from pipeline.target_schema import TargetColumn
from pipeline.lookups import LookupUse
from pipeline.transform import transform_batch, TransformContext, RULE_BUILDERS

LOAD_TIME = datetime(2026, 11, 19, 12, 0, 0)


# ---------------------------------------------------------------- helpers
def rule(dst, src, rule, **params):
    return ColumnRule(dst=dst, src=src, rule=rule, params=params)


def table(columns, raw=None, name="t"):
    return TableMapping(name=name, src_table="src", tgt_table="tgt", depends_on=[], columns=columns,
                        default={}, unmapped_src=[], raw=raw or {})


def col(name, pg_type, nullable=True, default=None, generated=False):
    return TargetColumn(name=name, pg_type=pg_type, nullable=nullable, default_expr=default, generated=generated)


class FakeLookups:
    def __init__(self, frames):
        self.frames = frames

    def get(self, use):
        return self.frames[use]


def ctx(schema, lookups=None, references=None):
    return TransformContext(load_time=LOAD_TIME, target_schema=schema, lookups=lookups,
                            references=references or {})


def run(df, m, c):
    return transform_batch(df, m, c)


# ---------------------------------------------------------------- tests
def test_every_rule_has_a_builder():
    missing = set(RULES) - set(RULE_BUILDERS) - {"lookup", "omit"}
    assert not missing, f"rules without a builder: {missing}"


def test_copy_and_widening_cast():
    df = pl.DataFrame({"a": ["x", None], "n": pl.Series([1, 2], dtype=pl.Int16)})
    m = table([rule("a", "a", "copy"), rule("n", "n", "cast", to="integer")])
    r = run(df, m, ctx({"a": col("a", "text"), "n": col("n", "integer")}))
    assert r.rows["a"].to_list() == ["x", None]
    assert r.rows["n"].dtype == pl.Int32 and r.rows["n"].to_list() == [1, 2]
    assert r.exceptions.height == 0


def test_float_to_numeric_uses_the_decimal_text():
    df = pl.DataFrame({"f": [0.1, 2.5, None]})
    m = table([rule("p", "f", "cast", to="numeric(10,5)")])
    r = run(df, m, ctx({"p": col("p", "numeric(10,5)")}))
    assert r.rows["p"].to_list() == [Decimal("0.10000"), Decimal("2.50000"), None]


def test_value_too_big_for_numeric_is_reported_not_truncated():
    df = pl.DataFrame({"k": [1, 2], "f": [1.5, 123456.5]})
    m = table([rule("p", "f", "cast", to="numeric(10,5)")], raw={"source": {"natural_key": ["k"]}})
    r = run(df, m, ctx({"p": col("p", "numeric(10,5)")}))
    assert r.rows.height == 1
    assert r.exceptions.to_dicts() == [{"reason": "value_does_not_fit_type", "column": "p", "key": "2"}]


def test_datetime_to_date():
    df = pl.DataFrame({"d": [datetime(2026, 1, 2, 3, 4)]})
    m = table([rule("dd", "d", "cast", to="date")])
    r = run(df, m, ctx({"dd": col("dd", "date")}))
    assert str(r.rows["dd"][0]) == "2026-01-02"


def test_trim():
    df = pl.DataFrame({"t": ["Daily     ", "x"]})
    m = table([rule("t", "t", "trim")])
    r = run(df, m, ctx({"t": col("t", "text")}))
    assert r.rows["t"].to_list() == ["Daily", "x"]


def test_coalesce_fallback_column_literal_and_load_time():
    df = pl.DataFrame({
        "iface": [None, "I2"], "own": ["O1", "O2"],
        "by": [None, "bob"], "ts": [None, datetime(2026, 1, 1)],
    })
    m = table([rule("interface_id", "iface", "coalesce", fallback_col="own"),
               rule("updated_by", "by", "coalesce", fallback="pct"),
               rule("updated_at", "ts", "coalesce", fallback="load_time")])
    r = run(df, m, ctx({"interface_id": col("interface_id", "text"), "updated_by": col("updated_by", "text"),
                        "updated_at": col("updated_at", "timestamp without time zone")}))
    assert r.rows["interface_id"].to_list() == ["O1", "I2"]
    assert r.rows["updated_by"].to_list() == ["pct", "bob"]
    assert r.rows["updated_at"].to_list() == [LOAD_TIME, datetime(2026, 1, 1)]


def test_bool_from_number_keeps_null():
    df = pl.DataFrame({"b": [0, 1, 5, None]})
    m = table([rule("f", "b", "bool")])
    r = run(df, m, ctx({"f": col("f", "boolean")}))
    assert r.rows["f"].to_list() == [False, True, True, None]


def test_bool_from_text():
    df = pl.DataFrame({"s": ["1", "Y", "y", "0", "N", "n", "maybe", None]})
    m = table([rule("f", "s", "bool_from_text", true_values=["1", "Y", "y"], false_values=["0", "N", "n"])])
    r = run(df, m, ctx({"f": col("f", "boolean")}))
    assert r.rows["f"].to_list() == [True, True, True, False, False, False, None, None]


def test_constant_set_null_and_load_time():
    df = pl.DataFrame({"x": [1, 2]})
    m = table([rule("by", None, "constant", value="pct"), rule("n", None, "set_null"),
               rule("t", None, "load_time"), rule("z", None, "constant", value=0)])
    r = run(df, m, ctx({"by": col("by", "text"), "n": col("n", "text"),
                        "t": col("t", "timestamp without time zone"), "z": col("z", "numeric(10,5)")}))
    assert r.rows["by"].to_list() == ["pct", "pct"]
    assert r.rows["n"].to_list() == [None, None]
    assert r.rows["t"].to_list() == [LOAD_TIME, LOAD_TIME]
    assert r.rows["z"].to_list() == [Decimal("0.00000")] * 2


def test_map_status_active_inactive():
    df = pl.DataFrame({"st": pl.Series([1, 0, 2, None], dtype=pl.Int16)})
    m = table([rule("status", "st", "map", values={1: "Active"}, **{"else": "Inactive"})])
    r = run(df, m, ctx({"status": col("status", "text")}))
    assert r.rows["status"].to_list() == ["Active", "Inactive", "Inactive", "Inactive"]


def test_exists_in_reference_table():
    df = pl.DataFrame({"downid": ["NO_DATA", "other", None, "sci"]})
    ref = {"PredefinedDownCodeInfo": pl.DataFrame({"DownId": ["NO_DATA", "SCI"]})}
    m = table([rule("is_predefined", "downid", "exists_in", table="PredefinedDownCodeInfo", column="DownId")])
    r = run(df, m, ctx({"is_predefined": col("is_predefined", "boolean")}, references=ref))
    assert r.rows["is_predefined"].to_list() == [True, False, False, True]


def test_when_applies_the_rule_only_on_matching_rows():
    df = pl.DataFrame({"dt": [1, 2, 1], "v": [10, 20, 30]})
    m = table([rule("pc", "v", "cast", to="integer", when={"column": "dt", "in": [1]})])
    r = run(df, m, ctx({"pc": col("pc", "integer")}))
    assert r.rows["pc"].to_list() == [10, None, 30]


def test_null_as_ddl_default():
    df = pl.DataFrame({"a": pl.Series([None, 3], dtype=pl.Int32), "b": pl.Series([None, True], dtype=pl.Boolean),
                       "c": pl.Series([None, None], dtype=pl.Int32)})
    m = table([rule("a", "a", "copy", null_as="ddl_default"),
               rule("b", "b", "copy", null_as="ddl_default"),
               rule("c", "c", "copy", null_as="ddl_default")])
    schema = {"a": col("a", "integer", default="0"), "b": col("b", "boolean", default="false"),
              "c": col("c", "integer")}                    # c has no default: stays NULL
    r = run(df, m, ctx(schema))
    assert r.rows["a"].to_list() == [0, 3]
    assert r.rows["b"].to_list() == [False, True]
    assert r.rows["c"].to_list() == [None, None]


def test_lookup_hit_miss_and_null_key():
    df = pl.DataFrame({"mid": ["M1", "M9", None]})
    frames = {LookupUse("machine_information", ("machine_id",), "iot_id"):
              pl.DataFrame({"machine_id": ["M1", "M2"], "iot_id": pl.Series([11, 12], dtype=pl.Int32)})}
    m = table([rule("machine_iot_id", "mid", "lookup", table="machine_information",
                    match={"machine_id": "mid"}, **{"return": "iot_id"})], name="holiday")
    c = ctx({"machine_iot_id": col("machine_iot_id", "integer")}, lookups=FakeLookups(frames))
    r = run(df, m, c)
    assert r.rows["machine_iot_id"].to_list() == [11, None, None]
    assert c.lookup_misses["holiday.machine_iot_id"] == 1          # M9 only; a NULL key is not a miss


def test_report_miss_false_gives_null_without_counting_a_miss():
    df = pl.DataFrame({"cust": ["AMIT", "NONE"]})
    frames = {LookupUse("customer_information", ("customer_name",), "iot_id"):
              pl.DataFrame({"customer_name": ["AMIT"], "iot_id": pl.Series([706], dtype=pl.Int32)})}
    m = table([rule("customer_iot_id", "cust", "lookup", table="customer_information",
                    match={"customer_name": "cust"}, report_miss=False, **{"return": "iot_id"})], name="component")
    c = ctx({"customer_iot_id": col("customer_iot_id", "integer")}, lookups=FakeLookups(frames))
    r = run(df, m, c)
    assert r.rows["customer_iot_id"].to_list() == [706, None]
    assert c.lookup_misses == {}


def test_on_miss_drop_removes_rows_without_a_parent_and_lists_them():
    df = pl.DataFrame({"mid": ["M1", "MC-03", None]})
    frames = {LookupUse("machine_information", ("machine_id",), "iot_id"):
              pl.DataFrame({"machine_id": ["M1"], "iot_id": pl.Series([11], dtype=pl.Int32)})}
    m = table([rule("machine_iot_id", "mid", "lookup", table="machine_information",
                    match={"machine_id": "mid"}, on_miss="drop", **{"return": "iot_id"}),
               rule("mid", "mid", "copy")],
              raw={"source": {"natural_key": ["mid"]}}, name="machine_make")
    c = ctx({"machine_iot_id": col("machine_iot_id", "integer"), "mid": col("mid", "text")}, lookups=FakeLookups(frames))
    r = run(df, m, c)
    assert r.rows["mid"].to_list() == ["M1", None]                   # the row with a NULL key is not an unmatched key
    assert r.exceptions.to_dicts() == [{"reason": "dropped_no_parent", "column": "machine_iot_id", "key": "MC-03"}]
    assert c.lookup_misses == {}                                       # reported once, as an exception


def test_on_miss_values_are_validated():
    from pipeline.mapping_config import validate_mapping
    good = table([rule("x", "a", "lookup", table="t", match={"k": "a"}, on_miss="drop", **{"return": "iot_id"})])
    bad = table([rule("x", "a", "lookup", table="t", match={"k": "a"}, on_miss="explode", **{"return": "iot_id"})])
    both = table([rule("x", "a", "lookup", table="t", match={"k": "a"}, predefined_match={"d": "a"},
                       on_miss="drop", **{"return": "iot_id"})])
    for m in (good, bad, both):
        m.src_table = m.tgt_table = "x"
    assert validate_mapping(good, {"t", "t2"}) == []
    assert any("on_miss must be" in e for e in validate_mapping(bad, {"t"}))
    assert any("cannot be combined" in e for e in validate_mapping(both, {"t"}))


def test_lookup_can_use_an_earlier_resolved_column():
    # operation_iot_id: matched on the machine and component ids resolved by the two lookups before it
    df = pl.DataFrame({"mc": ["m"], "comp": ["c"], "opn": ["10"]})
    i32 = lambda v: pl.Series(v, dtype=pl.Int32)
    frames = {
        LookupUse("machine_information", ("interface_id",), "iot_id"):
            pl.DataFrame({"interface_id": ["m"], "iot_id": i32([1])}),
        LookupUse("component_information", ("interface_id",), "iot_id"):
            pl.DataFrame({"interface_id": ["c"], "iot_id": i32([2])}),
        LookupUse("component_operation_pricing", ("machine_iot_id", "component_iot_id", "interface_id"), "iot_id"):
            pl.DataFrame({"machine_iot_id": i32([1, 1]), "component_iot_id": i32([2, 3]),
                          "interface_id": ["10", "10"], "iot_id": i32([100, 200])}),
    }
    m = table([
        rule("machine_iot_id", "mc", "lookup", table="machine_information", match={"interface_id": "mc"}, **{"return": "iot_id"}),
        rule("component_iot_id", "comp", "lookup", table="component_information", match={"interface_id": "comp"}, **{"return": "iot_id"}),
        rule("operation_iot_id", "opn", "lookup", table="component_operation_pricing",
             match={"machine_iot_id": "@machine_iot_id", "component_iot_id": "@component_iot_id", "interface_id": "opn"},
             **{"return": "iot_id"}),
    ])
    schema = {n: col(n, "integer") for n in ("machine_iot_id", "component_iot_id", "operation_iot_id")}
    r = run(df, m, ctx(schema, lookups=FakeLookups(frames)))
    assert r.rows.to_dicts() == [{"machine_iot_id": 1, "component_iot_id": 2, "operation_iot_id": 100}]


def test_lookup_with_duplicate_keys_is_refused_not_multiplied():
    df = pl.DataFrame({"mid": ["M1"]})
    frames = {LookupUse("machine_information", ("machine_id",), "iot_id"):
              pl.DataFrame({"machine_id": ["M1", "M1"], "iot_id": pl.Series([1, 2], dtype=pl.Int32)})}
    m = table([rule("x", "mid", "lookup", table="machine_information", match={"machine_id": "mid"}, **{"return": "iot_id"})])
    try:
        run(df, m, ctx({"x": col("x", "integer")}, lookups=FakeLookups(frames)))
    except ValueError as e:
        assert "multiplied" in str(e)
    else:
        raise AssertionError("a duplicate lookup key must raise")


def test_predefined_down_code_matches_on_down_id_others_on_interface_id():
    df = pl.DataFrame({"dcode": ["NO_DATA", "5", "99", None]})
    i32 = lambda v: pl.Series(v, dtype=pl.Int32)
    frames = {
        LookupUse("down_code_information", ("interface_id",), "iot_id"):
            pl.DataFrame({"interface_id": ["5", "NO_DATA"], "iot_id": i32([50, 999])}),     # 999 must NOT be used
        LookupUse("down_code_information", ("down_id",), "iot_id"):
            pl.DataFrame({"down_id": ["No_Data", "five"], "iot_id": i32([7, 50])}),
    }
    ref = {"PredefinedDownCodeInfo": pl.DataFrame({"DownId": ["NO_DATA"]})}
    m = table([rule("down_iot_id", "dcode", "lookup", table="down_code_information",
                    match={"interface_id": "dcode"}, predefined_match={"down_id": "dcode"},
                    predefined_names_from="PredefinedDownCodeInfo.DownId", case_sensitive=False,
                    **{"return": "iot_id"})])
    r = run(df, m, ctx({"down_iot_id": col("down_iot_id", "integer")}, lookups=FakeLookups(frames), references=ref))
    assert r.rows["down_iot_id"].to_list() == [7, 50, None, None]


def test_grouped_table_one_row_per_group():
    df = pl.DataFrame({"PlantID": ["a", "a", "b"], "GroupID": ["x", "x", "y"],
                       "description": [None, "D1", "D2"], "seq": [2, 1, 1],
                       "UpdatedTS": [datetime(2026, 1, 1), None, datetime(2026, 2, 2)]})
    m = table([rule("plant_id", "PlantID", "copy"), rule("group_id", "GroupID", "copy"),
               rule("description", "description", "first_non_null", order_by=["seq"]),
               rule("updated_at", "UpdatedTS", "max")],
              raw={"source": {"group_by": ["PlantID", "GroupID"], "natural_key": ["PlantID", "GroupID"]}})
    schema = {"plant_id": col("plant_id", "text"), "group_id": col("group_id", "text"),
              "description": col("description", "text"), "updated_at": col("updated_at", "timestamp without time zone")}
    r = run(df, m, ctx(schema))
    assert r.rows.to_dicts() == [
        {"plant_id": "a", "group_id": "x", "description": "D1", "updated_at": datetime(2026, 1, 1)},
        {"plant_id": "b", "group_id": "y", "description": "D2", "updated_at": datetime(2026, 2, 2)},
    ]


def test_null_in_not_null_column_goes_to_exceptions():
    df = pl.DataFrame({"k": ["r1", "r2"], "name": ["ok", None]})
    m = table([rule("name", "name", "copy")], raw={"source": {"natural_key": ["k"]}})
    r = run(df, m, ctx({"name": col("name", "text", nullable=False)}))
    assert r.rows["name"].to_list() == ["ok"]
    assert r.exceptions.to_dicts() == [{"reason": "null_in_not_null_column", "column": "name", "key": "r2"}]


def test_duplicate_primary_key_later_row_goes_to_exceptions():
    df = pl.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]})
    m = table([rule("k", "k", "copy"), rule("v", "v", "copy")],
              raw={"source": {"natural_key": ["k"]}, "target": {"primary_key": ["k"]}})
    r = run(df, m, ctx({"k": col("k", "text", nullable=False), "v": col("v", "bigint")}))
    assert r.rows["v"].to_list() == [1, 3]
    assert r.exceptions["reason"].to_list() == ["duplicate_key"]


def test_unique_key_allows_several_nulls():
    df = pl.DataFrame({"m": pl.Series([None, None, 5], dtype=pl.Int32)})
    m = table([rule("m", "m", "copy")], raw={"target": {"unique": [["m"]]}})
    r = run(df, m, ctx({"m": col("m", "integer")}))
    assert r.rows.height == 3 and r.exceptions.height == 0


def test_generated_and_omitted_columns_are_never_written():
    df = pl.DataFrame({"a": [1]})
    m = table([rule("a", "a", "copy"), rule("cycletime_range", None, "set_null"), rule("skip", None, "omit")])
    schema = {"a": col("a", "integer"), "cycletime_range": col("cycletime_range", "tsrange", generated=True),
              "skip": col("skip", "text")}
    r = run(df, m, ctx(schema))
    assert r.rows.columns == ["a"]


# ---------------------------------------------------------------- runner
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
