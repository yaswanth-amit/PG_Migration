"""Synthetic dbo.autodata rows for volume testing the migration (SQL Server side).

It learns the patterns from the rows that already exist (machine / component / operation / operator mix,
cycle times, load/unload gaps, down codes) and generates every case the real data has:

  * normal production cycles (datatype 1), part counts 1 / 2 / sometimes 0
  * McTI cycles: a long cycle whose cycletime is the machining time, with a McTI downtime inside it
  * ICD: a long cycle with a downtime inside it, also written to dbo.autodata_icd; the cycle's cycletime is
    its duration minus the ICD duration
  * background downtimes (datatype 2) with the down codes found in the data
  * NO_DATA rows for long gaps between cycles (rows can cross midnight)

Run from the project folder (it needs the same .env as main.py):

  python -m testdata.synthetic_autodata --rows 10000000            DRY RUN: generate, check, print; writes nothing
  python -m testdata.synthetic_autodata --rows 10000000 --execute  insert into the source database
  python -m testdata.synthetic_autodata --cleanup --execute        delete exactly the rows this tool inserted

Every inserted row carries the tag SYNTH_1CR (autodata.SplString2, autodata_icd.Remarks). Neither column is
migrated, and the tag is how --cleanup finds the rows. Rows are placed BEFORE the first real row, so the real
data and autodata_maxtime are untouched. Only counts and generated values are printed.
"""
import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

import numpy as np
import polars as pl

TAG = "SYNTH_1CR"
EPOCH = datetime(1970, 1, 1)
MCTI_CODE, NO_DATA_CODE = "McTI", "NO_DATA"
DOWN_CODES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "13", "UNKNOWN"]       # as found in the data
# share of units per kind; the rest are normal cycles
MIX = {"mcti": 0.003, "down": 0.009, "icd": 0.0015, "nodata": 0.0015}      # close to the real data, but every case is common at 1 crore rows
ROWS_PER_UNIT = {"normal": 1, "mcti": 2, "icd": 2, "down": 1, "nodata": 1}


@dataclass
class Combo:
    """One machine / component / operation / operator combination seen in the data."""
    mc: str
    comp: str
    opn: str
    opr: str
    weight: float
    cycletime: int                      # typical cycle time in seconds
    loadunload: int                     # typical gap before the cycle in seconds
    parts: int = 1
    mcti_cycletime: int | None = None   # set when the data shows McTI cycles for this combination
    mcti_total: int | None = None       # total duration of such a cycle


@dataclass
class Generated:
    autodata: pl.DataFrame
    icd: pl.DataFrame
    info: dict = field(default_factory=dict)


# ----------------------------------------------------------------------------- learn the pattern

def read_profile(conn) -> list[Combo]:
    """Combinations and their typical timings, from the existing data (read only)."""
    cur = conn.cursor()
    cur.execute("""SELECT mc, comp, opn, opr, cycletime, loadunload, PartsCount, DATEDIFF(second, sttime, ndtime)
                   FROM dbo.autodata WHERE datatype = 1""")
    cycles = pl.DataFrame([tuple(r) for r in cur.fetchall()], schema=["mc", "comp", "opn", "opr", "ct", "lu", "parts", "dur"],
                          orient="row", infer_schema_length=None)
    cur.execute("""SELECT c.mc, c.comp, c.opn, c.opr, c.cycletime, DATEDIFF(second, c.sttime, c.ndtime)
                   FROM dbo.autodata c WHERE c.datatype = 1 AND EXISTS (SELECT 1 FROM dbo.autodata d
                        WHERE d.mc = c.mc AND d.datatype = 2 AND d.dcode = ? AND d.sttime >= c.sttime AND d.sttime < c.ndtime)""",
                MCTI_CODE)
    mcti = pl.DataFrame([tuple(r) for r in cur.fetchall()], schema=["mc", "comp", "opn", "opr", "ct", "dur"],
                        orient="row", infer_schema_length=None)
    return profile_from_frames(cycles, mcti)


def profile_from_frames(cycles : pl.DataFrame , mcti : pl.DataFrame) -> list[Combo]:
    normal = cycles.filter(pl.col("ct") == pl.col("dur"))              # skip ICD / McTI cycles for the typical timing
    keys = ["mc", "comp", "opn", "opr"]
    total = max(normal.height, 1)
    combos = []
    for key, group in normal.group_by(keys, maintain_order=True):
        row = dict(zip(keys, key))
        parts = group["parts"].cast(pl.Int64).mode().sort().item(-1)       # most common part count (0 is the rare case)
        parts = int(parts) if parts else 1
        combo = Combo(**row, weight=group.height / total, cycletime=int(group["ct"].median()),
                      loadunload=int(group["lu"].median()), parts=parts)
        special = mcti.filter((pl.col("mc") == row["mc"]) & (pl.col("comp") == row["comp"]) & (pl.col("opn") == row["opn"]))
        if special.height:
            combo.mcti_cycletime, combo.mcti_total = int(special["ct"].median()), int(special["dur"].median())
        combos.append(combo)
    # McTI-only combinations (every cycle of them is a McTI cycle) still need an entry
    have = {(c.mc, c.comp, c.opn, c.opr) for c in combos}
    for key, group in mcti.group_by(keys, maintain_order=True):
        if tuple(key) not in have:
            combos.append(Combo(**dict(zip(keys, key)), weight=group.height / total, cycletime=int(group["ct"].median()),
                                loadunload=30, mcti_cycletime=int(group["ct"].median()), mcti_total=int(group["dur"].median())))
    return combos


# ----------------------------------------------------------------------------- generate

def _units(kinds : np.ndarray , combos : list[Combo] , rng : np.random.Generator) -> dict:
    """Turn a list of unit kinds into rows (as column arrays). Each unit starts at the end of the previous one."""
    n = len(kinds)
    normal_ix = [i for i, c in enumerate(combos) if c.mcti_cycletime is None]
    mcti_ix = [i for i, c in enumerate(combos) if c.mcti_cycletime is not None]
    w = np.array([combos[i].weight for i in normal_ix], dtype=float)
    combo = np.empty(n, dtype=np.int64)
    combo[:] = rng.choice(normal_ix, size=n, p=w / w.sum())
    is_mcti = kinds == "mcti"
    if mcti_ix:
        combo[is_mcti] = rng.choice(mcti_ix, size=int(is_mcti.sum()))
    ct0 = np.array([c.cycletime for c in combos])[combo]
    lu0 = np.array([c.loadunload for c in combos])[combo]
    lead = np.clip(np.rint(rng.normal(lu0, 3)), 1, 60).astype(np.int64)                 # gap before a cycle
    cyc = ct0 + rng.integers(0, 2, n)                                                   # cycle time, +0/+1 s like the data

    icd_len = np.clip(np.rint(rng.lognormal(np.log(900), 1.0, n)), 120, 16_777).astype(np.int64)
    start_off = np.maximum(10, ct0 - 2 + rng.integers(-3, 4, n))                         # ICD starts this long after the cycle
    down_len = np.clip(np.rint(rng.lognormal(np.log(1500), 1.2, n)), 52, 16_777).astype(np.int64)
    gap_len = rng.integers(1_800, 14_400, n)
    mcti_total = np.array([c.mcti_total or 0 for c in combos])[combo] + rng.integers(0, 2, n)
    mcti_ct = np.array([c.mcti_cycletime or 0 for c in combos])[combo]

    is_n, is_i, is_d, is_e = kinds == "normal", kinds == "icd", kinds == "down", kinds == "nodata"
    cycle_len = np.where(is_n, cyc, np.where(is_mcti, mcti_total, np.where(is_i, start_off + icd_len + 2, 0)))
    has_cycle = is_n | is_mcti | is_i
    advance = np.where(has_cycle, lead + cycle_len, np.where(is_d, down_len, gap_len))
    unit_start = np.concatenate([[0], np.cumsum(advance)[:-1]])                         # seconds from the timeline start
    cyc_st = unit_start + lead

    dcode_pick = np.array(DOWN_CODES, dtype=object)[rng.integers(0, len(DOWN_CODES), n)]
    parts = np.array([c.parts for c in combos])[combo]
    parts = np.where(rng.random(n) < 0.01, 0, parts)                                    # rare zero part count, as in the data

    # ---- cycle rows
    ix = np.flatnonzero(has_cycle)
    cycles = dict(unit=ix, combo=combo[ix], datatype=np.ones(len(ix), np.int64), dcode=np.full(len(ix), None, dtype=object),
                  st=cyc_st[ix], nd=cyc_st[ix] + cycle_len[ix],
                  ct=np.where(is_n, cyc, np.where(is_mcti, mcti_ct, start_off + 2))[ix], lu=lead[ix],
                  mst=unit_start[ix], parts=parts[ix], icd=np.zeros(len(ix), bool))
    # ---- McTI downtime inside a McTI cycle: from machining time - 1 s after the start to 1 s before the end
    m = np.flatnonzero(is_mcti)
    mcti_rows = dict(unit=m, combo=combo[m], datatype=np.full(len(m), 2), dcode=np.full(len(m), MCTI_CODE, dtype=object),
                     st=cyc_st[m] + mcti_ct[m] - 1, nd=cyc_st[m] + mcti_total[m] - 1, ct=np.zeros(len(m), np.int64),
                     lu=None, mst=None, parts=parts[m], icd=np.zeros(len(m), bool))
    mcti_rows["lu"] = mcti_rows["nd"] - mcti_rows["st"]          # a downtime row: loadunload = its duration, msttime = its start
    mcti_rows["mst"] = mcti_rows["st"]
    # ---- ICD downtime inside an ICD cycle (also goes to autodata_icd)
    k = np.flatnonzero(is_i)
    icd_rows = dict(unit=k, combo=combo[k], datatype=np.full(len(k), 2), dcode=dcode_pick[k],
                    st=cyc_st[k] + start_off[k], nd=cyc_st[k] + start_off[k] + icd_len[k], ct=np.zeros(len(k), np.int64),
                    lu=icd_len[k], mst=cyc_st[k] + start_off[k], parts=parts[k], icd=np.ones(len(k), bool))
    # ---- background downtime and NO_DATA gaps
    d = np.flatnonzero(is_d)
    down_rows = dict(unit=d, combo=combo[d], datatype=np.full(len(d), 2), dcode=dcode_pick[d], st=unit_start[d],
                     nd=unit_start[d] + down_len[d], ct=np.zeros(len(d), np.int64), lu=down_len[d], mst=unit_start[d],
                     parts=parts[d], icd=np.zeros(len(d), bool))
    e = np.flatnonzero(is_e)
    gap_rows = dict(unit=e, combo=combo[e], datatype=np.full(len(e), 2), dcode=np.full(len(e), NO_DATA_CODE, dtype=object),
                    st=unit_start[e], nd=unit_start[e] + gap_len[e], ct=np.zeros(len(e), np.int64), lu=gap_len[e],
                    mst=unit_start[e], parts=parts[e], icd=np.zeros(len(e), bool))

    out = {key: np.concatenate([part[key] for part in (cycles, mcti_rows, icd_rows, down_rows, gap_rows)])
           for key in cycles}
    out["total_seconds"] = int(advance.sum())
    return out


def generate_machine(combos : list[Combo] , rows : int , rng : np.random.Generator , mix : dict = MIX) -> dict:
    """Exactly `rows` autodata rows for one machine, as column arrays with times in seconds from the start."""
    if not any(c.mcti_cycletime is None for c in combos):
        raise ValueError("the machine needs at least one normal combination")
    has_mcti = any(c.mcti_cycletime is not None for c in combos)
    names = ["mcti", "down", "icd", "nodata"]
    probs = [mix[n] if (n != "mcti" or has_mcti) else 0.0 for n in names]
    kinds = rng.choice(names + ["normal"], size=rows, p=probs + [1 - sum(probs)])
    per_unit = np.vectorize(ROWS_PER_UNIT.get)(kinds)
    cut = int(np.searchsorted(np.cumsum(per_unit), rows, side="right"))
    kinds = kinds[:cut]
    pad = rows - int(per_unit[:cut].sum())
    kinds = np.concatenate([kinds, np.full(pad, "normal")])                             # a 1-row unit fills the last rows exactly
    return _units(kinds, combos, rng)


def generate(combos : list[Combo] , total_rows : int , end_before : datetime , seed : int = 20261119) -> Generated:
    """All machines together, sorted by time, ending before `end_before`."""
    rng = np.random.default_rng(seed)
    machines = sorted({c.mc for c in combos})
    frames, spans = [], {}
    per_machine = [total_rows // len(machines) + (1 if i < total_rows % len(machines) else 0) for i in range(len(machines))]
    for mc, rows in zip(machines, per_machine):
        mine = [c for c in combos if c.mc == mc]
        part = generate_machine(mine, rows, rng)
        start = end_before - timedelta(seconds=part["total_seconds"] + 3_600)
        origin = int((start - EPOCH).total_seconds())
        spans[mc] = (start, part["total_seconds"] / 86_400 / 365.25)
        c = {k: part[k] for k in ("datatype", "dcode", "ct", "lu", "parts", "icd")}
        names = lambda attr: np.array([getattr(mine[i], attr) for i in part["combo"]], dtype=object)
        frames.append(pl.DataFrame({
            "mc": [mc] * len(part["st"]), "comp": names("comp"), "opn": names("opn"), "opr": names("opr"),
            "dcode": c["dcode"].tolist(), "datatype": c["datatype"].astype(np.int8),
            "st": part["st"] + origin, "nd": part["nd"] + origin, "mst": part["mst"] + origin,
            "cycletime": c["ct"], "loadunload": c["lu"], "parts": c["parts"], "is_icd_row": c["icd"]}))
    frame = pl.concat(frames)
    for src, dst in (("st", "sttime"), ("nd", "ndtime"), ("mst", "msttime")):
        frame = frame.with_columns(pl.from_epoch(pl.col(src), time_unit="s").alias(dst)).drop(src)
    frame = frame.sort(["sttime", "mc", "datatype"])
    frame = frame.with_columns(pl.col("sttime").dt.truncate("1d").alias("stdate"), pl.col("ndtime").dt.truncate("1d").alias("nddate"))
    icd = frame.filter(pl.col("is_icd_row")).drop("is_icd_row", "parts", "msttime")
    return Generated(autodata=frame.drop("is_icd_row"), icd=icd,
                     info={"spans": spans, "icd_rows": icd.height, "end_before": end_before})


# ----------------------------------------------------------------------------- check

def check(g : Generated , expected_rows : int) -> list[str]:
    """Rules the data must obey; an empty list means all hold."""
    df, problems = g.autodata, []
    if df.height != expected_rows:
        problems.append(f"{df.height} rows, expected {expected_rows}")
    if df.select(pl.struct("mc", "sttime").is_duplicated().any()).item():
        problems.append("duplicate (mc, sttime): the unique index would reject them")
    if df.filter(pl.col("ndtime") <= pl.col("sttime")).height:
        problems.append("a row ends before it starts")
    if df.filter(pl.col("msttime") > pl.col("sttime")).height:
        problems.append("msttime after sttime")
    cycles = df.filter(pl.col("datatype") == 1).sort(["mc", "sttime"])
    overlap = cycles.with_columns(pl.col("ndtime").shift(1).over("mc").alias("prev_nd")).filter(pl.col("sttime") < pl.col("prev_nd"))
    if overlap.height:
        problems.append(f"{overlap.height} cycles overlap the previous cycle")
    # every McTI / ICD downtime lies inside one cycle of its machine
    icd_keys = g.icd.select("mc", "sttime").with_columns(pl.lit(True).alias("_is_icd"))
    inner = (df.filter(pl.col("datatype") == 2).join(icd_keys, on=["mc", "sttime"], how="left")
               .filter((pl.col("dcode") == MCTI_CODE) | pl.col("_is_icd").is_not_null()))
    joined = inner.join_asof(cycles.select("mc", pl.col("sttime").alias("c_st"), pl.col("ndtime").alias("c_nd")).sort("c_st"),
                             left_on="sttime", right_on="c_st", by="mc")
    if joined.filter((pl.col("c_nd").is_null()) | (pl.col("ndtime") > pl.col("c_nd"))).height:
        problems.append("an in-cycle downtime is not inside a cycle")
    if g.icd.height != df.height and g.icd.filter(pl.col("datatype") != 2).height:
        problems.append("autodata_icd holds a row that is not a downtime")
    return problems


def summary(g : Generated) -> str:
    df = g.autodata
    lines = [f"autodata rows: {df.height:,}   autodata_icd rows: {g.icd.height:,}",
             f"first row: {df['sttime'].min()}   last row: {df['ndtime'].max()}"]
    for mc, (start, years) in g.info["spans"].items():
        lines.append(f"  machine {mc}: starts {start:%Y-%m-%d}  ({years:.1f} years of continuous running)")
    by = df.group_by("datatype", "dcode").len().sort("datatype", "dcode")
    lines.append("datatype / down code / rows:")
    lines += [f"  {r[0]}  {str(r[1]):8s} {r[2]:>10,}" for r in by.iter_rows()]
    cross = df.filter(pl.col("stdate") != pl.col("nddate")).height
    lines.append(f"rows that cross midnight: {cross:,}")
    return "\n".join(lines)


# ----------------------------------------------------------------------------- write

AUTODATA_COLS = ["mc", "comp", "opn", "opr", "dcode", "stdate", "sttime", "nddate", "ndtime", "datatype", "cycletime",
                 "loadunload", "msttime", "PartsCount", "compslno", "post", "WorkOrderNumber", "SerialNumber", "HeatCode",
                 "SupervisorCode", "IsModified", "SplString2"]       # the other columns stay NULL, as in the real rows
ICD_COLS = ["mc", "comp", "opn", "opr", "dcode", "stdate", "sttime", "nddate", "ndtime", "datatype", "cycletime",
            "loadunload", "SerialNumber", "HeatCode", "Remarks"]


def _rows(df : pl.DataFrame , icd : bool):
    for r in df.iter_rows(named=True):
        base = (r["mc"], r["comp"], r["opn"], r["opr"], r["dcode"], r["stdate"], r["sttime"], r["nddate"], r["ndtime"],
                int(r["datatype"]), int(r["cycletime"]), int(r["loadunload"]))
        yield base + (("0", "0", TAG) if icd else (r["msttime"], float(r["parts"]), 0, 0, "0", "0", "0", " ", False, TAG))


def _insert(cur , table : str , cols : list[str] , df : pl.DataFrame , icd : bool , chunk : int = 100_000 , on_chunk=None) -> int:
    import pyodbc
    sql_text = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
    cur.fast_executemany = True
    sizes = {c: 50 for c in ("mc", "comp", "opn", "opr", "dcode", "SplString2", "SerialNumber", "HeatCode", "Remarks",
                             "WorkOrderNumber", "SupervisorCode")}
    done = 0
    for offset in range(0, df.height, chunk):
        part = df.slice(offset, chunk)
        cur.setinputsizes([(pyodbc.SQL_WVARCHAR, sizes[c], 0) if c in sizes else
                           (pyodbc.SQL_TYPE_TIMESTAMP, 0, 3) if c in ("stdate", "sttime", "nddate", "ndtime", "msttime") else
                           (pyodbc.SQL_DOUBLE, 0, 0) if c == "PartsCount" else (pyodbc.SQL_BIT, 0, 0) if c == "IsModified" else
                           (pyodbc.SQL_BIGINT, 0, 0) if c in ("cycletime", "loadunload", "datatype", "compslno", "post") else
                           (pyodbc.SQL_WVARCHAR, 50, 0) for c in cols])
        cur.executemany(sql_text, list(_rows(part, icd)))
        done += part.height
        if on_chunk:
            on_chunk(done)
    return done


def execute_insert(conn , g : Generated , table : str = "dbo.autodata" , icd_table : str = "dbo.autodata_icd" ,
                   disable_trigger : bool = True , chunk : int = 100_000 , log=print) -> None:
    """One transaction per chunk. The trigger on autodata handles one row per insert only, and every new row is
    older than autodata_maxtime, so it is switched off for the load and always switched on again."""
    cur = conn.cursor()
    if disable_trigger:
        cur.execute("DISABLE TRIGGER dbo.tr_autodata_maxtime ON dbo.autodata")
        conn.commit()
    try:
        _insert(cur, table, AUTODATA_COLS, g.autodata, icd=False, chunk=chunk,
                on_chunk=lambda n: (conn.commit(), log(f"autodata: {n:,} / {g.autodata.height:,}")))
        _insert(cur, icd_table, ICD_COLS, g.icd, icd=True, chunk=chunk)
        conn.commit()
    finally:
        if disable_trigger:
            conn.rollback()
            cur.execute("ENABLE TRIGGER dbo.tr_autodata_maxtime ON dbo.autodata")
            conn.commit()


def cleanup(conn , log=print) -> None:
    cur = conn.cursor()
    for table, col in (("dbo.autodata_icd", "Remarks"), ("dbo.autodata", "SplString2")):
        total = 0
        while True:
            cur.execute(f"DELETE TOP (200000) FROM {table} WHERE {col} = ?", TAG)
            total += cur.rowcount
            conn.commit()
            if cur.rowcount == 0:
                break
        log(f"{table}: deleted {total:,} tagged rows")


def tagged_counts(conn) -> tuple[int, int]:
    cur = conn.cursor()
    cur.execute("SELECT COUNT_BIG(*) FROM dbo.autodata WHERE SplString2 = ?", TAG)
    a = cur.fetchone()[0]
    cur.execute("SELECT COUNT_BIG(*) FROM dbo.autodata_icd WHERE Remarks = ?", TAG)
    return a, cur.fetchone()[0]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=10_000_000, help="autodata rows to generate (default 1 crore)")
    p.add_argument("--seed", type=int, default=20261119)
    p.add_argument("--execute", action="store_true", help="really write (insert, or delete with --cleanup); default is a dry run")
    p.add_argument("--cleanup", action="store_true", help="delete the rows this tool inserted")
    p.add_argument("--chunk", type=int, default=100_000)
    args = p.parse_args(argv)

    from pipeline.connections import source_connection
    with source_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME()")
        print("database:", cur.fetchone()[0])
        tagged = tagged_counts(conn)
        print(f"rows already tagged {TAG}: autodata={tagged[0]:,} autodata_icd={tagged[1]:,}")
        if args.cleanup:
            if args.execute:
                cleanup(conn)
            else:
                print("dry run: --cleanup --execute would delete those rows")
            return 0
        if tagged[0] and args.execute:
            print("ERROR: tagged rows already exist. Run --cleanup --execute first (the tool never adds twice).")
            return 2

        combos = read_profile(conn)
        cur.execute("SELECT MIN(sttime) FROM dbo.autodata WHERE SplString2 IS NULL OR SplString2 <> ?", TAG)
        first_real = cur.fetchone()[0]
        print(f"learned {len(combos)} machine/component/operation/operator combinations; first real row {first_real}")
        g = generate(combos, args.rows, end_before=first_real.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1), seed=args.seed)
        problems = check(g, args.rows)
        print(summary(g))
        if problems:
            print("CHECK FAILED:\n  " + "\n  ".join(problems))
            return 1
        print("all checks passed")
        if not args.execute:
            print("DRY RUN: nothing was written. Add --execute to insert.")
            return 0
        execute_insert(conn, g, chunk=args.chunk)
        print("done. After the load run ALTER INDEX/UPDATE STATISTICS if you like; --cleanup --execute removes it all.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
