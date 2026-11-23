"""Tests for batching.py (logical-month batches) and for batch-by-batch is_icd. No database is used: a fake target
connection answers utility.get_logical_day the way the real function does (a logical day starts at 06:00).
Run with:  python -m pytest tests/test_batching.py
"""
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import polars as pl
import pytest

from pipeline import batching, extract as ex

SHIFT_START = timedelta(hours=6)


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.row = conn, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        if "shift_details" in sql:
            self.row = (self.conn.running_shifts,)
            return
        if "pg_proc" in sql:
            self.row = (self.conn.function_exists,)
            return
        moment, date_only, which = params
        day = datetime(moment.year, moment.month, moment.day)
        start = day + SHIFT_START
        if not date_only and moment < start:
            start -= timedelta(days=1)
        self.row = (start if which == "start" else start + timedelta(days=1),)

    def fetchone(self):
        return self.row


class FakeConn:
    def __init__(self, function_exists=True, running_shifts=3):
        self.function_exists, self.running_shifts = function_exists, running_shifts

    def cursor(self):
        return FakeCursor(self)


def windows(low, high, **kw):
    return batching.logical_month_windows(FakeConn(), low, high, **kw)


def test_batches_are_logical_months_from_the_logical_start_to_the_logical_end():
    w = windows(datetime(2026, 10, 3, 9, 0), datetime(2026, 12, 20, 15, 0))
    assert w[:-1] == [(datetime(2026, 10, 1, 6), datetime(2026, 11, 1, 6)),
                      (datetime(2026, 11, 1, 6), datetime(2026, 12, 1, 6)),
                      (datetime(2026, 12, 1, 6), datetime(2027, 1, 1, 6))]
    assert w[-1] == (None, None)


def test_batches_never_overlap_and_leave_no_gap():
    w = windows(datetime(2024, 2, 1, 21, 38), datetime(2026, 11, 18, 17, 57))[:-1]
    assert all(a[1] == b[0] for a, b in zip(w, w[1:]))
    assert w[0][0] <= datetime(2024, 2, 1, 21, 38) < w[0][1] and w[-1][0] <= datetime(2026, 11, 18, 17, 57) < w[-1][1]


def test_a_row_before_the_logical_day_start_belongs_to_the_previous_month():
    w = windows(datetime(2026, 10, 1, 3, 0), datetime(2026, 10, 15, 12, 0))[:-1]
    assert w[0][0] == datetime(2026, 9, 1, 6) and w[-1][1] == datetime(2026, 11, 1, 6)


def test_a_row_exactly_at_a_boundary_is_in_the_next_batch_only():
    w = windows(datetime(2026, 10, 15), datetime(2026, 11, 1, 6, 0))[:-1]
    boundary = datetime(2026, 11, 1, 6)
    owners = [x for x in w if x[0] <= boundary < x[1]]
    assert len(owners) == 1 and owners[0][0] == boundary


def test_the_range_chosen_by_the_user_is_clipped_to_whole_logical_days():
    w = windows(datetime(2026, 1, 1), datetime(2026, 12, 31), start=datetime(2026, 10, 12), end=datetime(2026, 11, 3, 23, 59))[:-1]
    assert w == [(datetime(2026, 10, 12, 6), datetime(2026, 11, 1, 6)), (datetime(2026, 11, 1, 6), datetime(2026, 11, 4, 6))]


def test_several_months_per_batch_and_the_year_rollover():
    w = windows(datetime(2026, 11, 5), datetime(2027, 3, 1), months=3)[:-1]
    assert w[0] == (datetime(2026, 11, 1, 6), datetime(2027, 2, 1, 6)) and w[1][0] == datetime(2027, 2, 1, 6)
    assert w[-1][1] == datetime(2027, 3, 1, 6)           # high is 00:00 on 1 Mar: still the logical day of 28 Feb


def test_an_empty_source_gives_only_the_null_batch_and_the_null_batch_can_be_switched_off():
    assert windows(None, None) == [(None, None)]
    assert windows(None, None, include_null_window=False) == []


def test_a_missing_function_is_a_clear_error_and_never_a_silent_fallback():
    with pytest.raises(ValueError, match="get_logical_day"):
        batching.logical_month_windows(FakeConn(function_exists=False), datetime(2026, 1, 1), datetime(2026, 2, 1))


def test_no_running_shift_in_the_master_is_a_clear_error_not_a_batch_a_day_short():
    with pytest.raises(ValueError, match="shift_details"):
        batching.logical_month_windows(FakeConn(running_shifts=0), datetime(2026, 11, 3), datetime(2026, 11, 30, 12))


def test_a_shift_setup_that_does_not_move_forward_is_refused():
    class StuckCursor(FakeCursor):
        def execute(self, sql, params=()):
            super().execute(sql, params)
            if "pg_proc" not in sql and "shift_details" not in sql and params[2] == "end":
                self.row = (datetime(2020, 1, 1),)             # an 'end' that is before the start

    class Stuck(FakeConn):
        def cursor(self):
            return StuckCursor(self)

    with pytest.raises(ValueError):
        batching.logical_month_windows(Stuck(), datetime(2026, 1, 1), datetime(2026, 3, 1))


# ----------------------------------------------------------------------------- batch-by-batch is_icd
def frame(rows):
    return pl.DataFrame({"mc": [r[0] for r in rows], "datatype": [r[1] for r in rows], "sttime": [r[2] for r in rows],
                         "ndtime": [r[3] for r in rows], "cycletime": [r[4] for r in rows]},
                        schema={"mc": pl.String, "datatype": pl.Int64, "sttime": pl.Datetime, "ndtime": pl.Datetime, "cycletime": pl.Int64})


def candidates(rows):
    cyc = rows.filter((pl.col("datatype") == 1) & ((pl.col("ndtime") - pl.col("sttime")).dt.total_seconds() > pl.col("cycletime")))
    return cyc.select("mc", "sttime", "ndtime")


def lookback_of(rows):
    c = candidates(rows)
    return int((c["ndtime"] - c["sttime"]).dt.total_seconds().max()) if c.height else 0


def batch_cycles(all_rows, start, end, lookback):
    """What extract_batch_icd_cycles returns, as plain Polars: candidate cycles starting in [start - lookback - 2 s, end)."""
    c = candidates(all_rows)
    low = start - timedelta(seconds=lookback + ex.ICD_LOOKBACK_MARGIN_SECONDS)
    return c.filter((pl.col("sttime") >= low) & (pl.col("sttime") < end))


def flags_by_batch(rows, cuts):
    lb, out = lookback_of(rows), []
    for start, end in zip(cuts, cuts[1:]):
        owned = rows.filter((pl.col("sttime") >= start) & (pl.col("sttime") < end))
        out.append(ex.add_is_icd(owned, batch_cycles(rows, start, end, lb), lb))
    return pl.concat(out)


def flags_at_once(rows):
    return ex.add_is_icd(rows, candidates(rows), lookback_of(rows))


def key(df):
    return df.sort("mc", "sttime", "ndtime", "datatype").select("mc", "sttime", "ndtime", "datatype", "is_icd")


def test_the_example_a_cycle_over_a_boundary_flags_the_icds_of_the_next_batch():
    d = datetime(2026, 1, 1)
    rows = frame([("M1", 1, d, d + timedelta(hours=7), 3_600),                                    # 00:00 - 07:00
                  ("M1", 2, d + timedelta(hours=6, minutes=45), d + timedelta(hours=6, minutes=50), 0),
                  ("M1", 2, d + timedelta(hours=6, minutes=55), d + timedelta(hours=6, minutes=58), 0),
                  ("M1", 1, d + timedelta(hours=7, minutes=1), d + timedelta(hours=7, minutes=4), 180)])
    cuts = [d - timedelta(days=1) + timedelta(hours=6), d + timedelta(hours=6), d + timedelta(days=1, hours=6)]
    by_batch = key(flags_by_batch(rows, cuts))
    assert by_batch.equals(key(flags_at_once(rows)))
    assert by_batch.filter(pl.col("datatype") == 2)["is_icd"].to_list() == [1, 1]
    assert by_batch.filter(pl.col("datatype") == 1)["is_icd"].to_list() == [0, 0]


def test_an_icd_in_the_next_batch_is_not_flagged_without_the_context_cycle():
    d = datetime(2026, 1, 1)
    rows = frame([("M1", 1, d, d + timedelta(hours=7), 3_600), ("M1", 2, d + timedelta(hours=6, minutes=45), d + timedelta(hours=6, minutes=50), 0)])
    batch2 = rows.filter(pl.col("datatype") == 2)
    assert ex.add_is_icd(batch2, pl.DataFrame(schema={"mc": pl.String, "sttime": pl.Datetime, "ndtime": pl.Datetime}), 0)["is_icd"].to_list() == [0]


def test_batch_by_batch_gives_the_same_flags_as_all_at_once_on_random_data():
    rnd = random.Random(11)
    base = datetime(2026, 1, 1, 6)
    for _ in range(20):
        rows = []
        for mc in ("M1", "M2"):
            t = base
            for _ in range(250):
                t += timedelta(seconds=rnd.randint(1, 4_000))
                dur = rnd.choice([60, 90, 300, 3_000, 30_000, 70_000])
                rows.append((mc, 1, t, t + timedelta(seconds=dur), rnd.choice([60, dur, dur, 90])))
                for _ in range(rnd.choice([0, 0, 1, 2])):
                    s = t + timedelta(seconds=rnd.randint(-5, dur + 5))
                    rows.append((mc, 2, s, s + timedelta(seconds=rnd.randint(1, 4_000)), 0))
                t += timedelta(seconds=dur)
        df = frame(rows).unique(subset=["mc", "sttime"], keep="first")
        cuts = [base - timedelta(days=1)] + [base + timedelta(days=k * 5) for k in range(0, 40)]
        assert key(flags_by_batch(df, cuts)).equals(key(flags_at_once(df)))
