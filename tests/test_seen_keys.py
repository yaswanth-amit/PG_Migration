from datetime import datetime

import polars as pl

from pipeline import runner
from pipeline.mapping_config import TableMapping
from pipeline.transform import TransformResult, duplicate_key_sets, _empty_exceptions


def table(dedupe=None, unique=None) -> TableMapping:
    target = {"primary_key": ["id"], "unique": unique or [], "dedupe": dedupe or []}
    return TableMapping("t", "T", "t", [], [], {}, [], {"target": target})


def result(rows: list[tuple]) -> TransformResult:
    df = pl.DataFrame(rows, schema={"id": pl.Int64, "machine_iot_id": pl.Int64, "st_time": pl.Datetime}, orient="row")
    return TransformResult(rows=df, exceptions=_empty_exceptions())


T1, T2 = datetime(2026, 1, 1, 6), datetime(2026, 1, 1, 7)


def test_dedupe_keys_are_part_of_the_key_sets():
    assert duplicate_key_sets(table(dedupe=[["machine_iot_id", "st_time"]])) == [["id"], ["machine_iot_id", "st_time"]]


def test_a_key_seen_in_an_earlier_chunk_becomes_an_exception_and_the_first_row_stays():
    seen = runner.SeenKeys(table(dedupe=[["machine_iot_id", "st_time"]]))
    first = seen.filter(result([(1, 5, T1), (2, 5, T2)]))
    assert first.rows.height == 2 and first.exceptions.height == 0
    second = seen.filter(result([(3, 5, T1), (4, 6, T1)]))             # id 3 repeats (5, T1)
    assert second.rows["id"].to_list() == [4]
    assert second.exceptions.to_dicts() == [{"reason": "duplicate_key", "column": "machine_iot_id,st_time",
                                             "key": "5|2026-01-01 06:00:00.000000"}]


def test_rows_are_never_lost_read_equals_loaded_plus_exceptions():
    seen = runner.SeenKeys(table(dedupe=[["machine_iot_id", "st_time"]]))
    read = loaded = excepted = 0
    for chunk in ([(1, 5, T1), (2, 5, T2)], [(3, 5, T1), (4, 5, T2), (5, 7, T1)], [(6, 5, T1)]):
        r = seen.filter(result(chunk))
        read += len(chunk); loaded += r.rows.height; excepted += r.exceptions.height
    assert (read, loaded, excepted) == (6, 3, 3)


def test_null_keys_never_collide():
    seen = runner.SeenKeys(table(dedupe=[["machine_iot_id", "st_time"]]))
    seen.filter(result([(1, None, T1)]))
    again = seen.filter(result([(2, None, T1)]))
    assert again.rows.height == 1 and again.exceptions.height == 0


def test_a_new_load_starts_with_no_memory_of_the_old_one():
    m = table(dedupe=[["machine_iot_id", "st_time"]])
    runner.SeenKeys(m).filter(result([(1, 5, T1)]))
    assert runner.SeenKeys(m).filter(result([(2, 5, T1)])).rows.height == 1


def test_autodata_is_read_in_id_order_so_the_first_row_of_a_key_is_the_lowest_id():
    from pipeline import extract
    from pipeline.mapping_config import load_mappings
    sql = extract.build_select_sql(load_mappings().tables["autodata"], where="[sttime] >= ?")
    assert sql.endswith("ORDER BY [id]")


def test_the_targets_own_unique_keys_are_filtered_too():
    """employee_information: the target has UNIQUE(interface_id) that the YAML does not list."""
    from pipeline.transform import TransformContext
    m = table()
    ctx = TransformContext(load_time=T1, target_schema={}, unique_keys=[["interface_id"], ["id"]])
    assert duplicate_key_sets(m, ctx.unique_keys) == [["id"], ["interface_id"]]       # each key once
    seen = runner.SeenKeys(m, ctx)
    df = pl.DataFrame({"id": [1, 2, 3], "interface_id": ["a", "b", "a"]})
    first = seen.filter(TransformResult(rows=df.head(2), exceptions=_empty_exceptions()))
    second = seen.filter(TransformResult(rows=df.tail(1), exceptions=_empty_exceptions()))
    assert first.rows.height == 2 and second.rows.height == 0
    assert second.exceptions["column"].to_list() == ["interface_id"]


def test_transform_removes_a_repeat_of_a_target_unique_key_inside_a_chunk():
    from pipeline.mapping_config import load_mappings
    from pipeline.target_schema import TargetColumn
    from pipeline.transform import TransformContext, transform_batch
    m = load_mappings().tables["employee_information"]
    schema = {c.dst: TargetColumn(c.dst, "text", True) for c in m.columns}
    schema["is_admin"] = TargetColumn("is_admin", "boolean", True)
    schema["company_default"] = TargetColumn("company_default", "boolean", True)
    schema["updated_at"] = TargetColumn("updated_at", "timestamp without time zone", True)
    ctx = TransformContext(load_time=T1, target_schema=schema, unique_keys=[["interface_id"]])
    n = 2
    df = pl.DataFrame({"Employeeid": ["E1", "E2"], "interfaceid": ["20128", "20128"], "employeeno": [1, 2],
                       "Name": ["A", "B"], "designation": [None] * n, "qualification": [None] * n, "address1": [None] * n,
                       "address2": [None] * n, "phone": [None] * n, "Email": [None] * n, "MobileNo": [None] * n,
                       "status": [1, 1], "isadmin": [0, 0], "upassword": [None] * n, "LandingPage": [None] * n,
                       "Company_default": [None] * n, "Role": [None] * n, "UpdatedTS": [None] * n},
                      schema_overrides={"Company_default": pl.Boolean, "UpdatedTS": pl.Datetime, "designation": pl.String,
                                        "qualification": pl.String, "address1": pl.String, "address2": pl.String,
                                        "phone": pl.String, "Email": pl.String, "MobileNo": pl.String,
                                        "upassword": pl.String, "LandingPage": pl.String, "Role": pl.String})
    out = transform_batch(df, m, ctx)
    assert out.rows["employee_id"].to_list() == ["E1"]
    assert out.exceptions["reason"].to_list() == ["duplicate_key"]
