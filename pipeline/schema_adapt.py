"""Fit a reviewed mapping to the databases it meets.

Different customer databases may lack some columns (for example UpdatedTS, LandingPage). Before a table runs,
the columns of the real source table are read and every rule that needs a missing source column is left out:
the target column then gets its database default (or NULL). Each such rule is reported, never silent.
A missing column that a NOT NULL target (with no database default) depends on stops that table with a clear message.
"""
from dataclasses import dataclass, field, replace

from pipeline.mapping_config import ColumnRule, TableMapping


@dataclass
class Adaptation:
    table : str
    dropped : list[dict] = field(default_factory=list)      # {"dst", "rule", "missing": [...], "reason"}
    notes : list[str] = field(default_factory=list)
    blockers : list[str] = field(default_factory=list)


def list_source_columns(conn , m : TableMapping) -> set[str] | None:
    """Lower-case column names of the source table, or None when they cannot be read."""
    src = m.raw.get("source") or {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    (src.get("schema", "dbo"), m.src_table))
        names = {str(r[0]).lower() for r in cur.fetchall()}
        join = src.get("join")
        if names and join:                                  # the parent table's columns are named p.<column> in the mapping
            cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                        (join.get("schema", src.get("schema", "dbo")), join["table"]))
            names |= {f"p.{str(r[0]).lower()}" for r in cur.fetchall()}
        return names or None
    except Exception:                                       # noqa: BLE001 - unreadable = do not adapt
        return None


def _missing(col : ColumnRule , present : set[str]) -> list[str]:
    needed : list[str] = []
    if col.rule == "lookup":
        match = col.params.get("match")
        if isinstance(match, dict):
            needed += [v for v in match.values() if isinstance(v, str) and not v.startswith("@")]
    elif col.src:
        needed.append(col.src)
    when = col.params.get("when")
    if isinstance(when, dict) and when.get("column"):
        needed.append(when["column"])
    order_by = col.params.get("order_by")
    needed += order_by if isinstance(order_by, list) else ([order_by] if order_by else [])
    return [c for c in needed if isinstance(c, str) and c.lower() not in present]


def adapt_to_source(m : TableMapping , source_columns : set[str] | None , target_schema : dict | None = None
                    ) -> tuple[TableMapping, Adaptation]:
    """Return (mapping to use, what was left out). The reviewed mapping object itself is not changed."""
    result = Adaptation(table=m.name)
    if source_columns is None:
        return m, result

    keep : list[ColumnRule] = []
    for col in m.columns:
        params = dict(col.params)
        # an optional fallback column that is missing is just not used
        fb = params.get("fallback_col")
        if col.rule == "coalesce" and fb and str(fb).lower() not in source_columns:
            del params["fallback_col"]
            result.notes.append(f"{col.dst}: fallback column '{fb}' is not in the source, so it is not used")
            col = ColumnRule(col.dst, col.src, col.rule, params)

        missing = _missing(col, source_columns)
        if not missing:
            keep.append(col)
            continue

        reason = "source column missing: " + ", ".join(missing)
        result.dropped.append({"dst": col.dst, "rule": col.rule, "missing": missing, "reason": reason})
        tcol = (target_schema or {}).get(col.dst)
        if tcol is not None and not tcol.nullable and tcol.default_kind == "none" and not tcol.generated:
            result.blockers.append(
                f"{m.name}.{col.dst} is NOT NULL with no database default, but its source column "
                f"({', '.join(missing)}) is missing in this source database. Map '{col.dst}' to another "
                f"column or give it a value in the Mapping tab.")
        else:
            result.notes.append(f"{col.dst}: left to the database default ({reason})")

    if not result.dropped and not result.notes:
        return m, result
    return replace(m, columns=keep), result
