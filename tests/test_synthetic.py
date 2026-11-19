"""Offline tests for testdata/synthetic_autodata.py: no database is touched.
Run with:  python -m pytest tests/test_synthetic.py
"""
import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import polars as pl

from testdata import synthetic_autodata as sy

END = datetime(2026, 11, 2)
COMBOS = [
    sy.Combo("1001", "1001", "1020", "159", weight=0.5, cycletime=180, loadunload=30),
    sy.Combo("1001", "2001", "20", "158", weight=0.4, cycletime=71, loadunload=22, parts=2),
    sy.Combo("1001", "2002", "100", "159", weight=0.1, cycletime=60, loadunload=30, mcti_cycletime=60, mcti_total=300),
    sy.Combo("1002", "2001", "10", "159", weight=1.0, cycletime=70, loadunload=22),
]


def make(rows=20_000, seed=7):
    return sy.generate(COMBOS, rows, END, seed=seed)


def test_exact_row_count_and_all_checks_pass():
    g = make()
    assert g.autodata.height == 20_000
    assert sy.check(g, 20_000) == []


def test_every_case_is_present():
    g = make(60_000)
    df = g.autodata
    codes = set(df.filter(pl.col("datatype") == 2)["dcode"].to_list())
    assert {"McTI", "NO_DATA"} <= codes and codes & set(sy.DOWN_CODES)
    assert df.filter(pl.col("datatype") == 1).height > 0
    assert g.icd.height > 0 and set(g.icd["datatype"].to_list()) == {2}
    assert df.filter(pl.col("stdate") != pl.col("nddate")).height > 0           # rows that cross midnight
    assert set(df.filter(pl.col("datatype") == 1)["parts"].unique().to_list()) >= {0, 1, 2}


def test_rows_follow_the_patterns_of_the_real_data():
    df = make().autodata
    cycles = df.filter(pl.col("datatype") == 1)
    assert cycles.filter(pl.col("msttime") != pl.col("sttime") - pl.duration(seconds=pl.col("loadunload"))).height == 0
    downs = df.filter(pl.col("datatype") == 2)
    assert downs.filter(pl.col("cycletime") != 0).height == 0
    assert downs.filter(pl.col("loadunload") != (pl.col("ndtime") - pl.col("sttime")).dt.total_seconds()).height == 0
    assert downs.filter(pl.col("msttime") != pl.col("sttime")).height == 0
    normal = cycles.filter(pl.col("cycletime") == (pl.col("ndtime") - pl.col("sttime")).dt.total_seconds())
    assert normal.height > 0.9 * cycles.height


def test_icd_cycle_time_is_duration_minus_the_icd_and_the_row_is_mirrored():
    g = make(60_000)
    cycles = g.autodata.filter(pl.col("datatype") == 1).sort("mc", "sttime")
    hit = g.icd.join_asof(cycles.select("mc", pl.col("sttime").alias("c_st"), pl.col("ndtime").alias("c_nd"), pl.col("cycletime").alias("c_ct")),
                          left_on="sttime", right_on="c_st", by="mc")
    duration = (hit["c_nd"] - hit["c_st"]).dt.total_seconds()
    assert (duration - hit["loadunload"] - hit["c_ct"]).abs().max() == 0
    both = g.icd.join(g.autodata, on=["mc", "sttime", "ndtime", "dcode"], how="inner")
    assert both.height == g.icd.height


def test_same_seed_same_data_and_it_ends_before_the_real_data():
    a, b = make(5_000, seed=1), make(5_000, seed=1)
    assert a.autodata.equals(b.autodata)
    assert a.autodata["ndtime"].max() < END


def test_a_machine_without_a_normal_combination_is_refused():
    only_mcti = [sy.Combo("9", "a", "b", "c", 1.0, 60, 30, mcti_cycletime=60, mcti_total=300)]
    try:
        sy.generate(only_mcti, 100, END)
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_a_broken_dataset_is_caught_by_the_checks():
    g = make(2_000)
    broken = g.autodata.with_columns(pl.when(pl.int_range(pl.len()) == 5).then(pl.col("sttime").shift(5)).otherwise(pl.col("sttime")).alias("sttime"))
    assert sy.check(sy.Generated(broken, g.icd, g.info), 2_000)


def test_profile_learns_typical_values_and_mcti_cycles():
    cycles = pl.DataFrame({"mc": ["1"] * 4, "comp": ["a"] * 3 + ["b"], "opn": ["10"] * 3 + ["100"], "opr": ["159"] * 4,
                           "ct": [70, 71, 71, 60], "lu": [20, 22, 24, 30], "parts": [1, 1, 2, 1], "dur": [70, 71, 71, 300]})
    mcti = pl.DataFrame({"mc": ["1"], "comp": ["b"], "opn": ["100"], "opr": ["159"], "ct": [60], "dur": [300]})
    combos = {c.comp: c for c in sy.profile_from_frames(cycles, mcti)}
    assert combos["a"].cycletime == 71 and combos["a"].loadunload == 22 and combos["a"].parts == 1
    assert combos["b"].mcti_cycletime == 60 and combos["b"].mcti_total == 300


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok    {name}")
        except Exception as e:                    # noqa: BLE001
            failed += 1
            print(f"  FAIL  {name}: {type(e).__name__}: {e}")
    raise SystemExit(1 if failed else 0)
