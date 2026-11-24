from datetime import datetime

import polars as pl

from pipeline import maxtime as mt
from pipeline.transform import TransformResult


def rows(*items):
    """items: (machine, datatype, st, nd, workorder)"""
    return pl.DataFrame({"machine_iot_id": [i[0] for i in items], "datatype": [i[1] for i in items],
                         "st_time": [i[2] for i in items], "nd_time": [i[3] for i in items],
                         "workorder_number": [i[4] for i in items]})


def t(h, m=0):
    return datetime(2026, 1, 1, h, m)


def test_keeps_latest_row_per_machine_across_chunks():
    tracker = mt.MaxTimeTracker()
    tracker.update(rows((1, 1, t(1), t(2), "a"), (2, 1, t(1), t(3), "b")))
    tracker.update(rows((1, 1, t(5), t(6), "c"), (2, 1, t(0), t(1), "old")))      # machine 2: older, must not win
    got = {r["machine_iot_id"]: r["workorder_number"] for r in tracker.frame.iter_rows(named=True)}
    assert got == {1: "c", 2: "b"}


def test_only_datatype_1_and_2_and_machine_and_end_time_count():
    tracker = mt.MaxTimeTracker()
    tracker.update(rows((1, 1, t(1), t(2), "ok"), (1, 3, t(5), t(9), "other type"),
                        (None, 1, t(5), t(9), "no machine"), (2, 2, t(1), None, "no end")))
    assert tracker.frame.height == 1 and tracker.frame["workorder_number"][0] == "ok"


def test_tie_on_end_time_takes_later_start():
    tracker = mt.MaxTimeTracker()
    tracker.update(rows((1, 1, t(1), t(5), "early"), (1, 2, t(3), t(5), "late")))
    assert tracker.frame["workorder_number"][0] == "late"


def test_empty_or_incomplete_chunk_is_ignored():
    tracker = mt.MaxTimeTracker()
    tracker.update(pl.DataFrame({"machine_iot_id": [1]}))          # required columns missing
    tracker.update(rows((1, 3, t(1), t(2), "x")))                   # nothing qualifies
    assert tracker.frame is None


def test_tap_passes_results_through_and_collects():
    tracker = mt.MaxTimeTracker()
    results = [TransformResult(rows=rows((1, 1, t(1), t(2), "a")), exceptions=pl.DataFrame()),
               TransformResult(rows=rows((1, 1, t(3), t(4), "b")), exceptions=pl.DataFrame())]
    out = list(mt.tap(iter(results), tracker))
    assert out == results and tracker.frame["workorder_number"][0] == "b"


class FakeCursor:
    def __init__(self, log): self.log = log
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def executemany(self, query, rows): self.log.append((query.as_string(None) if hasattr(query, "as_string") else str(query), list(rows)))


class FakeConn:
    def __init__(self): self.log = []
    def cursor(self): return FakeCursor(self.log)


def test_write_inserts_one_row_per_machine_with_the_not_older_guard():
    tracker = mt.MaxTimeTracker()
    tracker.update(rows((1, 1, t(1), t(2), "a"), (2, 1, t(1), t(3), "b")))
    conn = FakeConn()
    assert tracker.write(conn) == 2
    query, data = conn.log[0]
    assert "ON CONFLICT" in query and "nd_time <= EXCLUDED.nd_time" in query
    assert sorted(r[0] for r in data) == [1, 2]


def test_write_with_nothing_collected_writes_nothing():
    conn = FakeConn()
    assert mt.MaxTimeTracker().write(conn) == 0 and conn.log == []
