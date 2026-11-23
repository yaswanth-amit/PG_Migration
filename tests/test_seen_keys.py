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
