import re
from dataclasses import dataclass

import polars as pl

SCHEMA_SQL = """
SELECT a.attnum                                  AS pos,
       a.attname                                 AS name,
       format_type(a.atttypid, a.atttypmod)      AS pg_type,
       NOT a.attnotnull                          AS nullable,
       pg_get_expr(d.adbin, d.adrelid)           AS default_expr,
       a.attgenerated <> ''                      AS is_generated,
       a.attidentity  <> ''                      AS is_identity
FROM pg_attribute a
JOIN pg_class     c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = %s
  AND c.relname = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""

# Kinds of default a column can have
DEFAULT_NONE = "none"            # no default
DEFAULT_SEQUENCE = "sequence"    # nextval(...) or identity: the database generates the value
DEFAULT_NOW = "now"              # CURRENT_TIMESTAMP / now(): replaced by the run's load time
DEFAULT_LITERAL = "literal"      # a constant such as false, 0, 'day'::text
DEFAULT_EXPRESSION = "expression"  # anything else; only valid for generated columns


@dataclass
class TargetColumn:
    name : str
    pg_type : str
    nullable : bool
    default_expr : str | None = None     # the default exactly as Postgres reports it
    generated : bool = False             # GENERATED ALWAYS column: must never be written
    is_identity : bool = False

    @property
    def default_kind(self) -> str:
        return parse_default(self.default_expr, self.is_identity)[0]

    @property
    def default_value(self):
        """The Python value of a constant default (None for every other kind)."""
        kind, value = parse_default(self.default_expr, self.is_identity)
        return value if kind == DEFAULT_LITERAL else None

    @property
    def db_generates_value(self) -> bool:
        """True when the database fills this column itself (sequence, identity or GENERATED ALWAYS)."""
        return self.generated or self.default_kind == DEFAULT_SEQUENCE


_NOW_EXPRESSIONS = {"current_timestamp", "now()", "localtimestamp", "current_date"}


def parse_default(default_expr : str | None , is_identity : bool = False) -> tuple[str, object]:
    """Turn the default text from pg_get_expr into (kind, value)."""
    if is_identity:
        return DEFAULT_SEQUENCE, None
    if default_expr is None:
        return DEFAULT_NONE, None

    expr = default_expr.strip()
    low = expr.lower()

    if low.startswith("nextval("):
        return DEFAULT_SEQUENCE, None
    if low in _NOW_EXPRESSIONS:
        return DEFAULT_NOW, None
    if low == "true":
        return DEFAULT_LITERAL, True
    if low == "false":
        return DEFAULT_LITERAL, False
    if re.fullmatch(r"-?\d+", expr):
        return DEFAULT_LITERAL, int(expr)
    if re.fullmatch(r"-?\d+\.\d+", expr):
        return DEFAULT_LITERAL, float(expr)

    # 'text'::text  or  'text'::character varying  (a doubled quote '' is one quote)
    m = re.fullmatch(r"'(.*)'(::[\w\s\[\]]+)?", expr, flags=re.DOTALL)
    if m:
        return DEFAULT_LITERAL, m.group(1).replace("''", "'")

    return DEFAULT_EXPRESSION, None


def polars_dtype(pg_type : str) -> pl.DataType:
    """Map the type text from format_type() (for example 'numeric(10,5)') to a Polars dtype."""
    t = pg_type.strip().lower()

    if t.endswith("[]"):
        return pl.List(polars_dtype(t[:-2]))

    m = re.fullmatch(r"numeric\((\d+),\s*(\d+)\)", t)
    if m:
        return pl.Decimal(int(m.group(1)), int(m.group(2)))
    if t == "numeric":
        # unconstrained numeric has no fixed scale in Postgres: 10 decimals is an assumption
        return pl.Decimal(38, 10)

    if t == "smallint":
        return pl.Int16
    if t == "integer":
        return pl.Int32
    if t == "bigint":
        return pl.Int64
    if t == "boolean":
        return pl.Boolean
    if t == "date":
        return pl.Date
    if t == "real":
        return pl.Float32
    if t == "double precision":
        return pl.Float64
    if t == "text" or t.startswith("character varying") or t.startswith("character("):
        return pl.String
    if t.startswith("timestamp"):
        return pl.Datetime("us")

    raise ValueError(f"No Polars type for Postgres type '{pg_type}'")


UNIQUE_KEYS_SQL = """
SELECT array_agg(a.attname ORDER BY k.ord) AS columns
FROM pg_index i
JOIN pg_class     c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
WHERE n.nspname = %s AND c.relname = %s
  AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indisvalid
  AND k.attnum > 0
GROUP BY i.indexrelid
"""


def read_unique_keys(pg_conn , schema : str , table : str) -> list[list[str]]:
    """Column sets that the target table keeps unique (primary key, unique constraints and unique indexes
    without a condition or expression). A repeat of such a key cannot be loaded, so the pipeline removes it from
    the data first instead of letting the COPY fail."""
    with pg_conn.cursor() as cur:
        cur.execute(UNIQUE_KEYS_SQL, (schema, table))
        return [list(row[0]) for row in cur.fetchall()]


def read_target_schema(pg_conn , schema : str , table : str) -> dict[str, TargetColumn]:
    with pg_conn.cursor() as cur:
        cur.execute(SCHEMA_SQL, (schema, table))
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Target table {schema}.{table} not found, or it has no columns.")

    columns : dict[str, TargetColumn] = {}
    for pos, name, pg_type, nullable, default_expr, is_generated, is_identity in rows:
        columns[name] = TargetColumn(
            name=name,
            pg_type=pg_type,
            nullable=nullable,
            default_expr=default_expr,
            generated=is_generated,
            is_identity=is_identity,
        )
    return columns


if __name__ == "__main__":
    # 1. Offline checks of the parsers (no database needed)
    assert parse_default("false") == (DEFAULT_LITERAL, False)
    assert parse_default("0") == (DEFAULT_LITERAL, 0)
    assert parse_default("''::text") == (DEFAULT_LITERAL, "")
    assert parse_default("'day'::text") == (DEFAULT_LITERAL, "day")
    assert parse_default("CURRENT_TIMESTAMP") == (DEFAULT_NOW, None)
    assert parse_default("nextval('master_data.shift_details_id_seq'::regclass)")[0] == DEFAULT_SEQUENCE
    assert parse_default(None) == (DEFAULT_NONE, None)
    assert parse_default("tsrange(mst_time, nd_time)")[0] == DEFAULT_EXPRESSION
    assert polars_dtype("numeric(10,5)") == pl.Decimal(10, 5)
    assert polars_dtype("timestamp without time zone") == pl.Datetime("us")
    assert polars_dtype("text[]") == pl.List(pl.String)
    print("parser checks passed")

    # 2. Read two real target tables; only column definitions are printed, never data
    from pipeline.connections import target_connection

    with target_connection() as conn:
        for schema, table in (("master_data", "shift_details"), ("production_core", "autodata")):
            cols = read_target_schema(conn, schema, table)
            print(f"\n{schema}.{table}: {len(cols)} columns")
            for c in cols.values():
                if c.generated or c.default_kind != DEFAULT_NONE or not c.nullable:
                    print(f"  {c.name:20s} {c.pg_type:30s} nullable={c.nullable!s:5} "
                          f"default={c.default_kind}({c.default_value!r}) generated={c.generated}")
