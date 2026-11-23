"""User choices that FILL GAPS in the reviewed mappings.

Rules of this layer:
  * A reviewed mapping can never be changed. A choice may only (a) map a source column that no mapping uses to a
    target column that no mapping fills, or (b) give a value to a target column that has no value.
  * Choices live in config/overrides/<table>.yaml, not in the reviewed YAML files.
  * Choices become ordinary column rules, so extract, transform and load work exactly as before.
"""
from datetime import datetime
from pathlib import Path

import yaml

from pipeline.mapping_config import BASE_DIR, ColumnRule, TableMapping
from pipeline.target_schema import TargetColumn

OVERRIDE_DIR = BASE_DIR / "config" / "overrides"

# What a user may choose for a target column
ALLOWED_RULES = {"copy", "cast", "bool", "trim", "coalesce", "constant", "load_time", "set_null", "omit"}
NEEDS_SOURCE = {"copy", "cast", "bool", "trim", "coalesce"}

# Source column "not decided" statuses used by the screens
MAPPED, NOT_MIGRATED, UNDECIDED, LEFT_BY_USER, MAPPED_BY_USER = (
    "mapped", "not migrated", "not decided", "left unmapped (user)", "mapped (user)")


# ----------------------------------------------------------------------------- type compatibility

_SQL_FAMILY = {
    "tinyint": "int", "smallint": "int", "int": "int", "bigint": "int",
    "decimal": "decimal", "numeric": "decimal", "money": "decimal", "smallmoney": "decimal",
    "float": "decimal", "real": "decimal",
    "bit": "bool",
    "char": "text", "varchar": "text", "nchar": "text", "nvarchar": "text", "text": "text", "ntext": "text",
    "date": "datetime", "datetime": "datetime", "datetime2": "datetime", "smalldatetime": "datetime",
    "datetimeoffset": "datetime",
    "uniqueidentifier": "guid", "varbinary": "binary", "binary": "binary", "image": "binary",
}
_INT_BITS = {"tinyint": 8, "smallint": 16, "int": 32, "bigint": 64}


def sql_family(sql_type : str) -> str:
    return _SQL_FAMILY.get((sql_type or "").lower(), "other")


def pg_family(pg_type : str) -> str:
    t = (pg_type or "").lower()
    if t in ("smallint", "integer", "bigint"):
        return "int"
    if t.startswith("numeric") or t in ("real", "double precision"):
        return "decimal"
    if t == "boolean":
        return "bool"
    if t == "text" or t.startswith("character"):
        return "text"
    if t.startswith("timestamp") or t == "date":
        return "datetime"
    if t == "uuid":
        return "guid"
    if t == "bytea":
        return "binary"
    return "other"


def compatibility(sql_type : str , pg_type : str) -> tuple[str, str, str | None]:
    """How a source type can fill a target type: (verdict, rule, message).
    verdict: 'ok' (copy as is), 'cast' (allowed, values that do not fit become exceptions), 'blocked'."""
    s, p = sql_family(sql_type), pg_family(pg_type)
    if s == p and s in ("int",):
        small = {"smallint": 16, "integer": 32, "bigint": 64}[pg_type.lower()]
        if _INT_BITS.get(sql_type.lower(), 64) > small:
            return "cast", "cast", f"{sql_type} may not fit {pg_type}; values that do not fit go to the exceptions report"
        return "ok", "copy", None
    if s == p and s in ("decimal", "datetime", "text", "guid", "binary"):
        if s == "decimal" and sql_type.lower() in ("float", "real") and pg_type.lower().startswith("numeric"):
            return "cast", "cast", "float to numeric: converted through its decimal text"
        return "ok", "copy", ("text may be longer than the target allows" if s == "text" and "(" in pg_type else None)
    if s == "bool" and p == "bool":
        return "ok", "bool", None
    if s == "bool" and p == "int":
        return "cast", "cast", None
    if s == "int" and p == "bool":
        return "cast", "bool", "0 = false, anything else = true"
    if s == "int" and p == "decimal":
        return "ok", "copy", None
    if s == "decimal" and p == "int":
        return "cast", "cast", "decimals are rounded down; values that do not fit go to the exceptions report"
    if s == "text" and p in ("int", "decimal", "datetime", "bool", "guid"):
        return "cast", "cast", "text must be parsable; values that are not go to the exceptions report"
    if p == "text" and s in ("int", "decimal", "bool", "datetime", "guid"):
        return "cast", "cast", None
    if s == "datetime" and p == "datetime":
        return "ok", "copy", None
    return "blocked", "", f"{sql_type} cannot be loaded into {pg_type}"


# ----------------------------------------------------------------------------- what is mapped, what is open

def used_source_columns(m : TableMapping) -> set[str]:
    from pipeline.extract import collect_source_columns
    return {c.lower() for c in collect_source_columns(m)}


def classify_source(m : TableMapping , source_columns : list[dict] , overrides : dict | None = None) -> list[dict]:
    """One row per SOURCE column with its status and details."""
    overrides = overrides or {}
    used = used_source_columns(m)
    declared = {str(c).split()[0].lower() for c in m.unmapped_src}
    by_user = {str(c.get("src")).lower(): c.get("dst") for c in overrides.get("columns", []) if c.get("src")}
    left = {str(c).lower() for c in overrides.get("leave_unmapped", [])}
    moved = _moved_source_columns(m)

    rows = []
    for col in source_columns:
        low = col["name"].lower()
        if low in by_user:
            status, note = MAPPED_BY_USER, f"→ {by_user[low]}"
        elif low in left:
            status, note = LEFT_BY_USER, ""
        elif low in used:
            status, note = MAPPED, ""
        elif low in declared:
            status, note = NOT_MIGRATED, "confirmed by the owner"
        elif low in moved:
            status, note = MAPPED, "used by another target of this table"
        else:
            status, note = UNDECIDED, ""
        rows.append({**col, "status": status, "note": note})
    return rows


def _moved_source_columns(m : TableMapping) -> set[str]:
    found : set[str] = set()
    for key, value in m.raw.items():
        if str(key).startswith("moved_to") and isinstance(value, (list, dict)):
            items = value if isinstance(value, list) else list(value)
            for item in items:
                found.add(str(item).split()[0].lower())
    return found


def classify_target(m : TableMapping , target_columns : dict[str, TargetColumn] , overrides : dict | None = None) -> list[dict]:
    """One row per TARGET column with its status ('mapped', 'fallback', 'no value yet', 'chosen by user', ...)."""
    overrides = overrides or {}
    by_user = {c.get("dst"): c for c in overrides.get("columns", [])}
    rules = {c.dst: c for c in m.columns}
    rows = []
    for name, col in target_columns.items():
        rule = rules.get(name)
        if name in by_user:
            status, how = "chosen by user", describe_rule(by_user[name])
        elif rule is not None:
            if rule.rule in ("set_null", "constant", "load_time"):
                status, how = "fixed value", describe_rule(_as_dict(rule))
            elif rule.rule == "coalesce":
                status, how = "fallback", describe_rule(_as_dict(rule))
            elif rule.rule == "omit":
                status, how = "database default", "not written; the database fills it"
            else:
                status, how = "mapped", describe_rule(_as_dict(rule))
        elif col.generated or col.default_kind == "sequence":
            status, how = "database generates", col.default_expr or "generated"
        elif name in (m.default or {}):
            status, how = "database default", str(m.default[name])
        else:
            status, how = "no value yet", ""
        rows.append({"name": name, "pg_type": col.pg_type, "nullable": col.nullable,
                     "default": col.default_expr or "", "status": status, "how": how,
                     "locked": rule is not None and name not in by_user})
    return rows


def _as_dict(rule : ColumnRule) -> dict:
    return {"dst": rule.dst, "src": rule.src, "rule": rule.rule, **rule.params}


def describe_rule(c : dict) -> str:
    rule = c.get("rule", "copy")
    src = c.get("src")
    if rule == "coalesce":
        fb = c.get("fallback_col") or c.get("fallback")
        return f"{src}, else {fb}"
    if rule == "lookup":
        return f"look up {src} in {c.get('table')}"
    if rule == "constant":
        return f"constant {c.get('value')!r}"
    if rule == "load_time":
        return "load time"
    if rule == "set_null":
        return "NULL"
    if rule == "omit":
        return "database default"
    if rule == "cast":
        return f"{src} as {c.get('to')}"
    return f"{rule} {src or ''}".strip()


# ----------------------------------------------------------------------------- files

def override_path(table : str , folder : Path | None = None) -> Path:
    return (folder or OVERRIDE_DIR) / f"{table}.yaml"


def load_override(table : str , folder : Path | None = None) -> dict:
    path = override_path(table, folder)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_override(table : str , data : dict , user : str , folder : Path | None = None) -> Path:
    """Write the choices of one table. An empty choice set removes the file (= reset)."""
    path = override_path(table, folder)
    if not data.get("columns") and not data.get("leave_unmapped"):
        path.unlink(missing_ok=True)
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {"table": table, "saved_by": user, "saved_at": datetime.now().isoformat(timespec="seconds"),
            "columns": data.get("columns", []), "leave_unmapped": data.get("leave_unmapped", [])}
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(body, f, sort_keys=False, allow_unicode=True)
    return path


# ----------------------------------------------------------------------------- merge + validation

def validate_override(m : TableMapping , data : dict , source_columns : list[dict] | None = None ,
                      target_columns : dict[str, TargetColumn] | None = None) -> list[str]:
    """Problems with the user's choices. Empty list = acceptable. Never lets a reviewed mapping change."""
    errors : list[str] = []
    reviewed_dst = {c.dst for c in m.columns}
    used = used_source_columns(m)
    src_by_name = {c["name"].lower(): c for c in (source_columns or [])}
    seen : set[str] = set()

    for i, c in enumerate(data.get("columns", []), start=1):
        dst, src, rule = c.get("dst"), c.get("src"), c.get("rule", "copy")
        where = f"{m.name}: choice #{i} ('{dst}')"
        if rule not in ALLOWED_RULES:
            errors.append(f"{where}: rule '{rule}' is not allowed for user choices.")
            continue
        if dst in reviewed_dst:
            errors.append(f"{where}: this column is already mapped by the reviewed mapping and cannot be changed.")
        if dst in seen:
            errors.append(f"{where}: target column chosen twice.")
        seen.add(dst)
        if target_columns is not None:
            tcol = target_columns.get(dst)
            if tcol is None:
                errors.append(f"{where}: target column does not exist.")
                continue
            if tcol.generated or tcol.default_kind == "sequence":
                errors.append(f"{where}: the database generates this column; it cannot be written.")
        if rule in NEEDS_SOURCE:
            if not src:
                errors.append(f"{where}: rule '{rule}' needs a source column.")
                continue
            if source_columns is not None:
                s = src_by_name.get(str(src).lower())
                if s is None:
                    errors.append(f"{where}: source column '{src}' does not exist.")
                    continue
                if str(src).lower() in used:
                    errors.append(f"{where}: source column '{src}' is already used by the reviewed mapping.")
                if target_columns is not None and dst in target_columns and rule in ("copy", "bool", "trim"):
                    verdict, _, msg = compatibility(s["type"], target_columns[dst].pg_type)
                    if verdict == "blocked":
                        errors.append(f"{where}: {msg}.")
                    elif verdict == "cast" and rule == "copy":
                        errors.append(f"{where}: {s['type']} → {target_columns[dst].pg_type} needs a cast.")
            if rule == "cast" and not c.get("to"):
                errors.append(f"{where}: cast needs a target type ('to').")
            if rule == "coalesce" and not (c.get("fallback_col") or "fallback" in c):
                errors.append(f"{where}: coalesce needs a fallback.")
        if rule == "constant" and "value" not in c:
            errors.append(f"{where}: constant needs a value.")
        if target_columns is not None and dst in target_columns:
            tcol = target_columns[dst]
            if rule in ("set_null",) and not tcol.nullable:
                errors.append(f"{where}: '{dst}' is NOT NULL, so it cannot be set to NULL.")
            if rule == "omit" and not tcol.nullable and tcol.default_kind == "none":
                errors.append(f"{where}: '{dst}' is NOT NULL and has no database default, so it needs a value.")
            if rule in ("copy", "cast", "bool", "trim") and not tcol.nullable and src and source_columns is not None:
                s = src_by_name.get(str(src).lower())
                if s and s.get("nullable"):
                    errors.append(f"{where}: '{dst}' is NOT NULL but '{src}' can be NULL; use 'coalesce' with a fallback.")

    for name in data.get("leave_unmapped", []):
        if str(name).lower() in used:
            errors.append(f"{m.name}: '{name}' is already mapped; it cannot be left unmapped.")
        if source_columns is not None and str(name).lower() not in src_by_name:
            errors.append(f"{m.name}: source column '{name}' does not exist.")
    for c in data.get("columns", []):
        if c.get("src") and str(c["src"]).lower() in {str(n).lower() for n in data.get("leave_unmapped", [])}:
            errors.append(f"{m.name}: '{c['src']}' is both mapped and left unmapped.")
    return errors


def apply_override(m : TableMapping , data : dict) -> None:
    """Add the user's choices to the table's column rules (call validate_override first)."""
    for c in data.get("columns", []):
        params = {k: v for k, v in c.items() if k not in ("dst", "src", "rule")}
        m.columns.append(ColumnRule(dst=c["dst"], src=c.get("src"), rule=c.get("rule", "copy"), params=params))


def load_reviewed():
    """The reviewed mappings only, without any user choice (what the screens compare the choices against)."""
    from pipeline.mapping_config import load_mappings
    return load_mappings(overrides_dir=BASE_DIR / "config" / "__no_overrides__")


def apply_overrides(tables : dict[str, TableMapping] , folder : Path | None = None) -> list[str]:
    """Merge every override file into the loaded mappings. Returns the errors found."""
    errors : list[str] = []
    for name, m in tables.items():
        data = load_override(name, folder)
        if not data:
            continue
        problems = validate_override(m, data)
        if problems:
            errors.extend(problems)
            continue
        apply_override(m, data)
    return errors
