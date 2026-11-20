"""Tests for the in-DataFrame is_icd rule (extract.add_is_icd): it must give exactly the answer of the SQL rule
(the s_GetCockpitData rule). No database is used; a plain-Python version of the SQL rule is the reference.
Run with:  python -m pytest tests/test_icd_frame.py
"""
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import polars as pl

from pipeline import extract as ex

T0 = datetime(2026, 1, 1)
LOOKBACK = 3_600


def at(seconds):
    return None if seconds is None else T0 + timedelta(seconds=seconds)


def rows(*items):
    """items: (mc, datatype, start_s, end_s)"""
    return pl.DataFrame({"mc": [i[0] for i in items], "datatype": [i[1] for i in items],
                         "sttime": [at(i[2]) for i in items], "ndtime": [at(i[3]) for i in items]},
                        schema={"mc": pl.String, "datatype": pl.Int64, "sttime": pl.Datetime, "ndtime": pl.Datetime})


def cycles(*items):
    return pl.DataFrame({"mc": [i[0] for i in items], "sttime": [at(i[1]) for i in items], "ndtime": [at(i[2]) for i in items]},
                        schema={"mc": pl.String, "sttime": pl.Datetime, "ndtime": pl.Datetime})


def reference(batch, cyc, lookback=LOOKBACK):
    """The SQL rule, row by row, in plain Python."""
    margin = lookback + ex.ICD_LOOKBACK_MARGIN_SECONDS
    out = []
    for a in batch.iter_rows(named=True):
        hit = 0
        if a["datatype"] == 2 and a["sttime"] is not None and a["ndtime"] is not None:
            low = min(a["sttime"], a["ndtime"]) - timedelta(seconds=margin)
            for c in cyc.iter_rows(named=True):
                if (c["mc"].lower().rstrip() == a["mc"].lower().rstrip() and c["sttime"] < a["sttime"] and c["sttime"] >= low
                        and a["sttime"] > c["sttime"] and a["ndtime"] < c["ndtime"]):
                    hit = 1
                    break
        out.append(hit)
    return out


def flagged(batch, cyc):
    return ex.add_is_icd(batch, cyc, LOOKBACK)["is_icd"].to_list()


def test_a_down_row_strictly_inside_a_long_cycle_is_an_icd():
    assert flagged(rows(("M1", 2, 100, 200)), cycles(("M1", 50, 300))) == [1]


def test_boundaries_are_strict():
    c = cycles(("M1", 100, 300))
    assert flagged(rows(("M1", 2, 100, 200)), c) == [0]          # starts together with the cycle
    assert flagged(rows(("M1", 2, 150, 300)), c) == [0]          # ends together with the cycle


def test_other_machine_other_datatype_and_null_times_are_not_icd():
    c = cycles(("M1", 50, 300))
    batch = rows(("M2", 2, 100, 200), ("M1", 1, 100, 200), ("M1", 2, None, 200), ("M1", 2, 100, None))
    assert flagged(batch, c) == [0, 0, 0, 0]


def test_machine_names_compare_like_sql_server_without_case_and_trailing_spaces():
    assert flagged(rows(("m1 ", 2, 100, 200)), cycles(("M1", 50, 300))) == [1]


def test_a_cycle_from_an_earlier_window_counts_and_one_beyond_the_lookback_does_not():
    far = cycles(("M1", 100, 100 + 2 * LOOKBACK))
    inside = cycles(("M1", 100, 100 + LOOKBACK))
    batch = rows(("M1", 2, 100 + LOOKBACK - 100, 100 + LOOKBACK - 50))
    assert flagged(batch, inside) == [1]
    far_batch = rows(("M1", 2, 100 + LOOKBACK + 50, 100 + LOOKBACK + 60))
    assert flagged(far_batch, far) == reference(far_batch, far)      # the lookback cut-off is the same as in SQL


def test_no_candidate_cycles_means_no_icd_and_the_column_is_still_added():
    out = ex.add_is_icd(rows(("M1", 2, 100, 200), ("M1", 1, 0, 50)), cycles(), LOOKBACK)
    assert out["is_icd"].to_list() == [0, 0] and out.columns[-1] == "is_icd" and out.height == 2


def test_the_row_order_and_other_columns_are_kept():
    batch = rows(("M1", 2, 100, 200), ("M2", 1, 5, 6), ("M1", 2, 400, 500))
    out = ex.add_is_icd(batch, cycles(("M1", 50, 300)), LOOKBACK)
    assert out.drop("is_icd").equals(batch) and out["is_icd"].to_list() == [1, 0, 0]


def test_random_data_gives_the_same_answer_as_the_sql_rule():
    rnd = random.Random(5)
    for _ in range(25):
        machines = ["M1", "m2", "M3 "]
        cyc = cycles(*[(rnd.choice(machines), s, s + rnd.randint(1, 2_000)) for s in (rnd.randint(0, 10_000) for _ in range(30))])
        items = []
        for _ in range(60):
            start = rnd.randint(0, 12_000) if rnd.random() > 0.03 else None
            end = None if (start is None or rnd.random() < 0.05) else start + rnd.randint(-50, 400)
            items.append((rnd.choice(machines), rnd.choice([1, 2, 2]), start, end))
        batch = rows(*items)
        assert flagged(batch, cyc) == reference(batch, cyc)
