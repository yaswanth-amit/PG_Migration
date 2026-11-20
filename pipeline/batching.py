"""Batches (windows) cut at LOGICAL-DAY boundaries, for the windowed table (autodata).

A logical day starts at the start of the first running shift (for example 06:00), as defined by
utility.get_logical_day in the TARGET database (it reads master_data.shift_details, so the shift master must be
migrated first; it always is, because masters run before autodata).

One batch is one logical month by default:
    start = get_logical_day(first day of the month, date only, 'start')     e.g. 2026-10-01 06:00
    end   = get_logical_day(last  day of the month, date only, 'end')       e.g. 2026-11-01 06:00
The end of a month is the start of the next one, so batches are half-open [start, end), never overlap and leave
no gap: every row belongs to exactly one batch (its st_time decides). The start of each batch after the first is
simply the end of the previous one, which guarantees that, whatever the shift setup is.
(If the shift master leaves a gap, for example only 06:00-22:00 is a running shift, the batch edge is the end of the
last running shift instead of 06:00. Batches stay contiguous, so no row is lost or counted twice.)
"""
import calendar
from datetime import date, datetime

MAX_BATCHES = 5_000                   # a guard against a wrong shift setup that never moves forward


def _function_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = 'utility' AND p.proname = 'get_logical_day')")
        return bool(cur.fetchone()[0])


def _running_shifts(conn) -> int:
    """How many running shifts the shift master has. With none, utility.get_logical_day falls back to midnight and
    returns the SAME date for 'end', which would cut every batch a day short."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM master_data.shift_details WHERE is_running = true")
        return int(cur.fetchone()[0])


def logical_bound(conn , when : datetime | date , which : str , date_only : bool) -> datetime:
    """utility.get_logical_day(when, date_only, 'start' | 'end'). date_only=True: the logical day OF that calendar
    date; False: the logical day that CONTAINS that moment (03:00 belongs to the previous logical day)."""
    moment = when if isinstance(when, datetime) else datetime(when.year, when.month, when.day)
    with conn.cursor() as cur:
        cur.execute("SELECT utility.get_logical_day(%s::timestamp, %s, %s)", (moment, date_only, which))
        value = cur.fetchone()[0]
    if value is None:
        raise ValueError(f"utility.get_logical_day returned nothing for {moment} ('{which}').")
    return value


def _month_last_day(first : date , months : int) -> date:
    """Last calendar day of the month that is (months - 1) months after the month of `first`."""
    index = first.year * 12 + (first.month - 1) + (months - 1)
    year, month = divmod(index, 12)
    return date(year, month + 1, calendar.monthrange(year, month + 1)[1])


def logical_month_windows(conn , low : datetime | None , high : datetime | None ,
                          start : datetime | None = None , end : datetime | None = None , months : int = 1 ,
                          include_null_window : bool = True) -> list[tuple[datetime | None, datetime | None]]:
    """Consecutive half-open batches [start, end) of `months` logical months covering low..high.
    start / end (the optional range chosen by the user) are DAYS: start = the logical start of that day,
    end = the logical end of that day. Without them the range is the logical days that hold low and high.
    (None, None) is the extra 'time is NULL' batch, so rows without a time are not lost."""
    if months < 1:
        raise ValueError("months must be at least 1")

    windows : list[tuple[datetime | None, datetime | None]] = []
    first_time, last_time = start or low, end or high
    if first_time is not None and last_time is not None:
        if not _function_exists(conn):
            raise ValueError("utility.get_logical_day does not exist in the target database; "
                             "batching by logical day needs it (and master_data.shift_details).")
        if _running_shifts(conn) == 0:
            raise ValueError("master_data.shift_details has no running shift in the target database, so the logical day "
                             "is not defined. Migrate shift_details before autodata (or use --days for fixed windows).")
        if start is not None:                                               # a range chosen by the user: that logical day on
            first_date = start.date()
            range_start = logical_bound(conn, first_date, "start", True)
        else:                                                               # whole months: from the 1st of the month of low
            low_date = logical_bound(conn, low, "start", False).date()      # (the logical day that contains low)
            first_date = date(low_date.year, low_date.month, 1)
            range_start = logical_bound(conn, first_date, "start", True)
        if end is not None:                                                 # a range chosen by the user: up to that logical day's end
            range_end = logical_bound(conn, end.date(), "end", True)
        else:                                                               # whole months: to the end of the month of high
            high_date = logical_bound(conn, high, "start", False).date()    # (the logical day that contains high)
            range_end = logical_bound(conn, _month_last_day(date(high_date.year, high_date.month, 1), 1), "end", True)

        current, day = range_start, first_date
        while current < range_end:
            if len(windows) >= MAX_BATCHES:
                raise ValueError(f"more than {MAX_BATCHES} batches: check the shift setup and the date range.")
            last_day = _month_last_day(day, months)
            batch_end = logical_bound(conn, last_day, "end", True)
            if batch_end <= current:
                raise ValueError(f"the logical day end {batch_end} does not move forward from {current}.")
            windows.append((current, min(batch_end, range_end)))
            current, day = batch_end, date.fromordinal(last_day.toordinal() + 1)

        if start is None and end is None:                  # the whole table: the batches must cover every time
            if not windows or not (windows[0][0] <= low and high < windows[-1][1]):
                raise ValueError(f"the batches {windows[:1]}..{windows[-1:]} do not cover the data from {low} to {high}: "
                                 "check the shift setup (utility.get_logical_day).")

    if include_null_window:
        windows.append((None, None))
    return windows
