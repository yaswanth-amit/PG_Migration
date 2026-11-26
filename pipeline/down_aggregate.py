"""The DOWN columns of shift_production_down_details, computed in Polars (mapping key down_aggregate).

The source table ShiftProductionDetails has no down columns. They are the sums of the rows of
ShiftDownTimeDetails that share the same key (date, shift, plant, group, machine, component, operation,
operator, work order, heat code), exactly as the live aggregation (etl.load_agg_prod_summary_from_autodata) builds them.

  net_downtime                     = SUM(DownTime)                                   (already without the planned part)
  down_planned_downtime            = SUM(PDT)
  management_loss                  = SUM(cap(DownTime))                              over rows with ML_Flag
  management_loss_planned_downtime = SUM(cap(DownTime + PDT) - cap(DownTime))        over rows with ML_Flag
  operator_loss                    = SUM(DownTime)                                   over rows with prodeffy and not ThresholdfromCO
  operator_loss_planned_downtime   = SUM(PDT)                                        over the same rows
  total_minor_downtime             = SUM(DownTime + PDT)                             over rows with PE_Flag   (gross: the live proc does not take the planned part off)
  cap(x) = Threshold when Threshold > 0 and x > Threshold, else x

How it is used, per window:
  1. DownAggregate.load reads the down rows of the window from SQL Server and sums them per key (one small frame).
  2. apply adds the 7 columns to every chunk of production rows (a join on the key; no match = 0).
  3. leftover returns the keys that have down rows but no production row, as new rows with zero production
     (the live table has such rows). They are yielded after the last chunk of the window.
"""
from datetime import datetime
from typing import Sequence

import polars as pl

DOWN_COLUMNS = [
    "net_downtime", "management_loss", "operator_loss", "total_minor_downtime",
    "down_planned_downtime", "management_loss_planned_downtime", "operator_loss_planned_downtime",
]
FLAG_COLUMNS = ["ML_Flag", "PE_Flag", "prodeffy", "ThresholdfromCO"]
VALUE_COLUMNS = ["DownTime", "PDT", "Threshold"]
KEY_NAME = "__down_key"
NULL_TEXT = "\x00null"          # a NULL part of a key must equal another NULL part, so it becomes a text


def key_expression(columns : Sequence[str] , zero_text : Sequence[str] = ()) -> pl.Expr:
    """One text key from several columns. A NULL work order or heat code counts as '0' (the mapping stores '0')."""
    parts = []
    for name in columns:
        expr = pl.col(name).cast(pl.String)
        expr = expr.fill_null("0") if name in zero_text else expr.fill_null(NULL_TEXT)
        parts.append(expr)
    return pl.concat_str(parts, separator="\x1f").alias(KEY_NAME)


def aggregate_down_rows(down : pl.DataFrame , key_columns : Sequence[str] , zero_text : Sequence[str]) -> pl.DataFrame:
    """One row per key with the 7 down columns. `down` has the source columns of ShiftDownTimeDetails."""
    threshold = pl.col("Threshold").cast(pl.Float64).fill_null(0.0)
    down_time = pl.col("DownTime").cast(pl.Float64).fill_null(0.0)
    planned = pl.col("PDT").cast(pl.Float64).fill_null(0.0)

    def cap(value : pl.Expr) -> pl.Expr:
        return pl.when((threshold > 0) & (value > threshold)).then(threshold).otherwise(value)

    def flag(name : str) -> pl.Expr:
        return pl.col(name).cast(pl.Boolean).fill_null(False)

    ml = flag("ML_Flag")
    operator = flag("prodeffy") & ~flag("ThresholdfromCO")
    minor = flag("PE_Flag")

    def total(expr : pl.Expr , rows : pl.Expr | None = None) -> pl.Expr:
        return (pl.when(rows).then(expr).otherwise(0.0) if rows is not None else expr).sum()

    carried = [pl.col(c).first() for c in key_columns] + [
        pl.col(c).max() for c in ("ShiftStart", "ShiftEnd") if c in down.columns]
    return (down.with_columns(key_expression(key_columns, zero_text))
                .group_by(KEY_NAME, maintain_order=True)
                .agg(carried + [
                    total(down_time).alias("net_downtime"),
                    total(cap(down_time), ml).alias("management_loss"),
                    total(down_time, operator).alias("operator_loss"),
                    total(down_time + planned, minor).alias("total_minor_downtime"),
                    total(planned).alias("down_planned_downtime"),
                    total(cap(down_time + planned) - cap(down_time), ml).alias("management_loss_planned_downtime"),
                    total(planned, operator).alias("operator_loss_planned_downtime"),
                ])
                .with_columns([pl.col(c).round(5) for c in DOWN_COLUMNS]))


class DownAggregate:
    """The down sums of ONE window, kept in memory while the production chunks of the window go by."""

    def __init__(self , config : dict , frame : pl.DataFrame):
        self.config = config
        self.key : dict[str, str] = dict(config["key"])                  # production column -> down column
        self.zero_text = [c for c in ("WorkOrderNumber", "HeatCode") if c in self.key]
        self.frame = frame                                               # one row per key, from aggregate_down_rows
        self.matched : list[pl.Series] = []
        self.extra_rows = 0                                              # rows added by leftover()

    # ---- reading the window
    @staticmethod
    def read_columns(config : dict) -> list[str]:
        columns = list(dict.fromkeys(list(config["key"].values()) + VALUE_COLUMNS + FLAG_COLUMNS))
        return columns + [c for c in (config.get("extra_row_columns") or {}).values() if c not in columns]

    @classmethod
    def empty(cls , config : dict):
        schema = {c: pl.String for c in cls.read_columns(config)}
        return cls(config, pl.DataFrame(schema=schema))

    @staticmethod
    def build_select_sql(config : dict , have : set[str]) -> str:
        """The SELECT of the down rows of one window. `have` = the lower-case column names of the down table.
        A flag column the down table does not have (an older schema has no prodeffy / ThresholdfromCO on the row)
        is taken from the down code master by DownID (flag_master), where the live procedure takes it from too."""
        columns = DownAggregate.read_columns(config)
        master = config.get("flag_master")
        missing = [c for c in columns if c in FLAG_COLUMNS and c.lower() not in have]
        if missing and not master:
            raise ValueError(f"The down table has no column {missing} and the mapping has no flag_master to take it from.")
        parts = [(f"m.[{c}] AS [{c}]" if c in missing else f"a.[{c}]") for c in columns]
        schema = config.get("source_schema", "dbo")
        sql = f"SELECT {', '.join(parts)} FROM [{schema}].[{config['source_table']}] AS a"
        if missing:
            on = " AND ".join(f"a.[{k}] = m.[{v}]" for k, v in master["match"].items())
            sql += f" LEFT JOIN [{schema}].[{master['table']}] AS m ON {on}"
        date_col = config["date_column"]
        return sql + f" WHERE a.[{date_col}] >= ? AND a.[{date_col}] < ?"

    @classmethod
    def load(cls , conn , config : dict , start : datetime | None , end : datetime | None):
        """Read the down rows of one window (dDate >= start AND dDate < end) and sum them. A window with no
        bounds (the NULL window) has no production rows to attach to, so it is empty."""
        if start is None or end is None:
            return cls.empty(config)
        schema = config.get("source_schema", "dbo")
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)", f"{schema}.{config['source_table']}")
            have = {r[0].lower() for r in cursor.fetchall()}
            cursor.execute(cls.build_select_sql(config, have), start, end)
            rows = cursor.fetchall()
            names = [d[0] for d in cursor.description]
        finally:
            cursor.close()
        if not rows:
            return cls.empty(config)
        down = pl.DataFrame([tuple(r) for r in rows], schema=names, orient="row", infer_schema_length=None)
        return cls.from_down_rows(config, down)

    @classmethod
    def from_down_rows(cls , config : dict , down : pl.DataFrame):
        """Sum a frame of down rows (source column names)."""
        key = dict(config["key"])
        zero_text = [c for c in ("WorkOrderNumber", "HeatCode") if c in key]
        if down.height == 0:
            return cls.empty(config)
        return cls(config, aggregate_down_rows(down, list(key.values()), zero_text))

    # ---- one production chunk
    def apply(self , batch : pl.DataFrame) -> pl.DataFrame:
        """Add the 7 down columns to a chunk of production rows. A key with no down rows gets 0."""
        keys = list(self.key.keys())
        key_expr = key_expression(keys, self.zero_text)
        right = self.frame.select([pl.col(KEY_NAME)] + [pl.col(c) for c in DOWN_COLUMNS]) \
            if self.frame.height else pl.DataFrame(schema={KEY_NAME: pl.String, **{c: pl.Float64 for c in DOWN_COLUMNS}})
        work = batch.with_columns(key_expr)
        self.matched.append(work[KEY_NAME].unique())
        joined = work.join(right, on=KEY_NAME, how="left")
        if joined.height != batch.height:
            raise ValueError("down aggregate multiplied the production rows: the down keys are not unique.")
        return joined.with_columns([pl.col(c).fill_null(0.0) for c in DOWN_COLUMNS]).drop(KEY_NAME)

    # ---- keys with no production row
    def leftover(self , columns : Sequence[str] , load_time : datetime | None = None) -> pl.DataFrame:
        """New production rows for the down keys that no production row matched, with the production source
        columns `columns` (the values the mapping does not know stay NULL; quantities and times become 0
        through null_as: zero in the mapping). A key without a machine is left out: it cannot be a row."""
        if self.frame.height == 0:
            return pl.DataFrame()
        seen = pl.concat(self.matched) if self.matched else pl.Series(KEY_NAME, [], dtype=pl.String)
        rest = self.frame.filter(~pl.col(KEY_NAME).is_in(seen.implode()))
        machine = self.key.get("MachineID")
        if machine:
            rest = rest.filter(pl.col(machine).is_not_null())
        if rest.height == 0:
            return pl.DataFrame()

        values = dict(self.config.get("extra_row_values") or {})
        extra_cols = dict(self.config.get("extra_row_columns") or {})
        out : dict[str, pl.Expr] = {}
        for name in columns:
            if name in self.key:
                out[name] = pl.col(self.key[name])
            elif name in extra_cols:
                out[name] = pl.col(extra_cols[name])
            elif name in values:
                value = values[name]
                out[name] = pl.lit(load_time or datetime.now()) if value == "now" else pl.lit(value)
            elif name in DOWN_COLUMNS:
                out[name] = pl.col(name)
            else:
                out[name] = pl.lit(None)
        frame = rest.select([expr.alias(name) for name, expr in out.items()] +
                            [pl.col(c) for c in DOWN_COLUMNS if c not in out])
        self.extra_rows = frame.height
        return frame
