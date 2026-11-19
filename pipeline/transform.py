from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import polars as pl

from pipeline.mapping_config import TableMapping, ColumnRule
from pipeline.target_schema import TargetColumn, polars_dtype, DEFAULT_LITERAL, DEFAULT_NOW
from pipeline.lookups import LookupUse

# While a batch is being built, every target column lives under this prefix. That keeps target
# names from clashing with source names (for example the target 'status' and the source 'status').
T = "__t__"
MISS_SUFFIX = "__miss"        # temporary flag column of a lookup with on_miss: drop


@dataclass
class TransformContext:
    load_time : datetime                              # one timestamp for the whole run
    target_schema : dict[str, TargetColumn]           # from target_schema.read_target_schema
    lookups : Any = None                              # a LookupCache (anything with .get(LookupUse))
    references : dict[str, pl.DataFrame] = field(default_factory=dict)   # for example PredefinedDownCodeInfo
    lookup_misses : dict[str, int] = field(default_factory=dict)         # "table.column" -> rows with no match


@dataclass
class TransformResult:
    rows : pl.DataFrame            # rows that can be loaded, columns in target order
    exceptions : pl.DataFrame      # rows that cannot be loaded: reason, column, key (natural key only)


# ----------------------------------------------------------------------------- rules
# Each rule builds a Polars expression from a ColumnRule. 'schema' holds the SOURCE dtypes.

def _rule_copy(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.col(col.src)


def _rule_cast(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    target = polars_dtype(col.params["to"])
    expr = pl.col(col.src)
    src_dtype = schema[col.src]

    # A float must go through its decimal text: casting the binary float directly can fail,
    # and the text ('0.1') is the value people see, not 0.1000000000000000055.
    if isinstance(target, pl.Decimal) and src_dtype in (pl.Float32, pl.Float64):
        expr = expr.cast(pl.String)

    if target == pl.Date and isinstance(src_dtype, pl.Datetime):
        return expr.dt.date()

    # strict=False turns a value that does not fit into NULL; the caller reports those rows.
    return expr.cast(target, strict=False)


def _rule_trim(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.col(col.src).str.strip_chars()


def _rule_coalesce(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    parts = [pl.col(col.src)]
    if "fallback_col" in col.params:
        parts.append(pl.col(col.params["fallback_col"]))
    if "fallback" in col.params:
        fallback = col.params["fallback"]
        parts.append(pl.lit(ctx.load_time) if fallback == "load_time" else pl.lit(fallback))
    return pl.coalesce(parts)


def _rule_bool(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.col(col.src).cast(pl.Boolean)        # 0 = false, non-zero = true, NULL stays NULL


def _rule_bool_from_text(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    text = pl.col(col.src).cast(pl.String).str.strip_chars()
    true_values = [str(v) for v in col.params["true_values"]]
    false_values = [str(v) for v in col.params["false_values"]]
    return (pl.when(text.is_in(true_values)).then(pl.lit(True))
              .when(text.is_in(false_values)).then(pl.lit(False))
              .otherwise(pl.lit(None, dtype=pl.Boolean)))


def _rule_constant(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.lit(col.params["value"])


def _rule_map(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    values = col.params["values"]
    text = pl.col(col.src).cast(pl.String)
    otherwise = pl.lit(col.params.get("else"))

    chain = None
    for source_value, mapped in values.items():
        condition = text == str(source_value)
        chain = pl.when(condition).then(pl.lit(mapped)) if chain is None else chain.when(condition).then(pl.lit(mapped))

    return otherwise if chain is None else chain.otherwise(otherwise)


def _rule_load_time(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.lit(ctx.load_time)


def _rule_set_null(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    return pl.lit(None)


def _rule_exists_in(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    ref_name = col.params["table"]
    if ref_name not in ctx.references:
        raise ValueError(f"Rule 'exists_in' needs the reference table '{ref_name}' in the context.")
    names = (ctx.references[ref_name][col.params["column"]]
             .drop_nulls().cast(pl.String).str.to_lowercase().to_list())
    return pl.col(col.src).cast(pl.String).str.to_lowercase().is_in(names).fill_null(False)


def _rule_aggregated(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    # first_non_null and max are computed in _group_table; here the column is already aggregated.
    return pl.col(col.src)


def _rule_derived(col : ColumnRule, ctx : TransformContext, schema : dict) -> pl.Expr:
    name = col.params["name"]
    if name not in schema:
        raise ValueError(f"Derived rule '{name}' needs a column called '{name}' from the extract step.")
    return pl.col(name)


RULE_BUILDERS = {
    "copy": _rule_copy,
    "cast": _rule_cast,
    "trim": _rule_trim,
    "coalesce": _rule_coalesce,
    "bool": _rule_bool,
    "bool_from_text": _rule_bool_from_text,
    "constant": _rule_constant,
    "map": _rule_map,
    "load_time": _rule_load_time,
    "set_null": _rule_set_null,
    "exists_in": _rule_exists_in,
    "first_non_null": _rule_aggregated,
    "max": _rule_aggregated,
    "derived": _rule_derived,
    # "lookup" is a join (see _apply_lookup) and "omit" writes nothing
}


def _apply_when(expr : pl.Expr , col : ColumnRule) -> pl.Expr:
    """when: {column: X, in: [..]} -> the rule applies only on those rows, NULL elsewhere."""
    when = col.params.get("when")
    if not when:
        return expr
    return pl.when(pl.col(when["column"]).is_in(when["in"])).then(expr).otherwise(None)


def _apply_null_as(expr : pl.Expr , col : ColumnRule , ctx : TransformContext) -> pl.Expr:
    """null_as: ddl_default -> a NULL becomes the column's default in the target database."""
    if col.params.get("null_as") != "ddl_default":
        return expr

    target_col = ctx.target_schema.get(col.dst)
    if target_col is None:
        return expr
    if target_col.default_kind == DEFAULT_LITERAL:
        return expr.fill_null(pl.lit(target_col.default_value))
    if target_col.default_kind == DEFAULT_NOW:
        return expr.fill_null(pl.lit(ctx.load_time))
    return expr            # no usable default: the NULL stays


# ----------------------------------------------------------------------------- lookups

def _left_name(value : str) -> str:
    """'@machine_iot_id' = a target column already resolved earlier; anything else is a source column."""
    return T + value[1:] if value.startswith("@") else value


def _join_lookup(work : pl.DataFrame , left_cols : list[str] , frame : pl.DataFrame ,
                 right_keys : list[str] , ret : str , out : str , lower : bool = False) -> tuple[pl.DataFrame, int]:
    """Left-join `ret` from `frame` onto `work` as column `out`. Returns (frame, number of misses)."""
    n = len(left_cols)

    # temporary key columns, typed like the lookup keys, so a NULL-typed batch column still joins
    key_exprs = []
    for i, left in enumerate(left_cols):
        expr = pl.col(left).cast(frame.schema[right_keys[i]], strict=False)
        if lower:
            expr = expr.str.to_lowercase()
        key_exprs.append(expr.alias(f"__lk{i}"))
    work = work.with_columns(key_exprs)

    right_exprs = []
    for i, key in enumerate(right_keys):
        expr = pl.col(key)
        if lower:
            expr = expr.str.to_lowercase()
        right_exprs.append(expr.alias(f"__rk{i}"))
    right = frame.select(right_exprs + [pl.col(ret).alias(out)])

    rows_before = work.height
    joined = work.join(right, left_on=[f"__lk{i}" for i in range(n)],
                       right_on=[f"__rk{i}" for i in range(n)], how="left")
    if joined.height != rows_before:
        raise ValueError(f"Lookup on {right_keys} multiplied the rows ({rows_before} -> {joined.height}): "
                         f"the lookup keys are not unique.")

    all_keys_present = pl.all_horizontal([pl.col(f"__lk{i}").is_not_null() for i in range(n)])
    misses = joined.filter(all_keys_present & pl.col(out).is_null()).height

    drop = [c for c in joined.columns if c.startswith("__lk") or c.startswith("__rk")]
    return joined.drop(drop), misses


def _apply_lookup(work : pl.DataFrame , col : ColumnRule , ctx : TransformContext , m : TableMapping) -> pl.DataFrame:
    if ctx.lookups is None:
        raise ValueError(f"Table '{m.name}' needs lookups but the context has no lookup cache.")

    params = col.params
    table, ret = params["table"], params["return"]
    out = T + col.dst
    drop_on_miss = params.get("on_miss") == "drop"
    if drop_on_miss and "predefined_match" in params:
        raise ValueError(f"Table '{m.name}', column '{col.dst}': on_miss: drop cannot be combined with predefined_match.")

    match = params["match"]
    keys = list(match.keys())
    left_cols = [_left_name(v) for v in match.values()]
    frame = ctx.lookups.get(LookupUse(table=table, keys=tuple(keys), ret=ret))
    joined, misses = _join_lookup(work, left_cols, frame, keys, ret, out)

    # on_miss: drop = a row whose key has no match in the parent table is not migrated (it is listed, with the
    # reason 'dropped_no_parent', so the drop is never silent). The flag is turned into a mask by transform_batch.
    if drop_on_miss:
        unmatched = pl.all_horizontal([pl.col(c).is_not_null() for c in left_cols]) & pl.col(out).is_null()
        joined = joined.with_columns(unmatched.alias(out + MISS_SUFFIX))

    # Predefined down codes (NO_DATA, SCI, ...) are matched on another column, by name.
    if "predefined_match" in params:
        pre_match = params["predefined_match"]
        pre_keys = list(pre_match.keys())
        pre_left = [_left_name(v) for v in pre_match.values()]
        pre_frame = ctx.lookups.get(LookupUse(table=table, keys=tuple(pre_keys), ret=ret))
        joined, _ = _join_lookup(joined, pre_left, pre_frame, pre_keys, ret, out + "__pre", lower=True)

        ref_table, ref_column = params["predefined_names_from"].split(".")
        names = (ctx.references[ref_table][ref_column]
                 .drop_nulls().cast(pl.String).str.to_lowercase().to_list())
        is_predefined = pl.col(pre_left[0]).cast(pl.String).str.to_lowercase().is_in(names).fill_null(False)
        joined = joined.with_columns(
            pl.when(is_predefined).then(pl.col(out + "__pre")).otherwise(pl.col(out)).alias(out)
        ).drop(out + "__pre")
        misses = joined.filter(pl.col(pre_left[0]).is_not_null() & pl.col(out).is_null()).height

    # report_miss: false = an unmatched value simply becomes NULL and is not counted or warned about.
    # A dropped row is reported through the exceptions, so it is not counted a second time here.
    if params.get("report_miss", True) and not drop_on_miss:
        name = f"{m.name}.{col.dst}"
        ctx.lookup_misses[name] = ctx.lookup_misses.get(name, 0) + misses
    return joined


# ----------------------------------------------------------------------------- grouping

def _group_table(df : pl.DataFrame , m : TableMapping) -> pl.DataFrame:
    """Tables built from a grouped source (for example plant_groups): one output row per group."""
    group_by = (m.raw.get("source") or {}).get("group_by")
    if not group_by:
        return df

    aggregations : dict[str, pl.Expr] = {}
    sort_by : list[str] = []
    for col in m.columns:
        if col.rule == "first_non_null":
            aggregations[col.src] = pl.col(col.src).drop_nulls().first()
            order_by = col.params.get("order_by") or []
            sort_by = list(order_by) if isinstance(order_by, list) else [order_by]
        elif col.rule == "max":
            aggregations[col.src] = pl.col(col.src).max()

    allowed = set(group_by) | set(aggregations)
    for col in m.columns:
        if col.src and col.src not in allowed:
            raise ValueError(f"Table '{m.name}': column '{col.dst}' reads '{col.src}', which is neither a "
                             f"group_by column nor aggregated.")

    if sort_by:
        df = df.sort(sort_by, nulls_last=True)
    return df.group_by(group_by, maintain_order=True).agg(list(aggregations.values()))


# ----------------------------------------------------------------------------- main

def _key_expr(work : pl.DataFrame , m : TableMapping) -> pl.Expr:
    """The natural key of a row as text, used to identify rows in the exceptions report."""
    natural_key = (m.raw.get("source") or {}).get("natural_key") or []
    present = [c for c in natural_key if c in work.columns]
    if not present:
        return pl.lit("")
    return pl.concat_str([pl.col(c).cast(pl.String) for c in present], separator="|", ignore_nulls=True)


def _empty_exceptions() -> pl.DataFrame:
    return pl.DataFrame(schema={"reason": pl.String, "column": pl.String, "key": pl.String})


def transform_batch(df : pl.DataFrame , m : TableMapping , ctx : TransformContext) -> TransformResult:
    work = _group_table(df, m)
    source_schema = dict(work.schema)

    out_cols : list[str] = []
    masks : list[tuple[str, str, str]] = []          # (mask column, reason, target column)

    # 1. apply the rules in file order (a later rule may refer to an earlier result with "@column")
    for col in m.columns:
        if col.rule == "omit":
            continue

        target_col = ctx.target_schema.get(col.dst)
        if target_col is None:
            raise ValueError(f"Table '{m.name}': target column '{col.dst}' does not exist in the target table.")
        if target_col.generated:
            continue                                  # a generated column must never be written

        if col.rule == "lookup":
            work = _apply_lookup(work, col, ctx, m)
            if col.params.get("on_miss") == "drop":
                mask_col = f"__m{len(masks)}"
                work = work.with_columns(pl.col(T + col.dst + MISS_SUFFIX).alias(mask_col))
                masks.append((mask_col, "dropped_no_parent", col.dst))
        else:
            builder = RULE_BUILDERS.get(col.rule)
            if builder is None:
                raise ValueError(f"Table '{m.name}', column '{col.dst}': no builder for rule '{col.rule}'.")

            base = builder(col, ctx, source_schema)
            expr = _apply_null_as(_apply_when(base, col), col, ctx)
            work = work.with_columns(expr.alias(T + col.dst))

            # A cast that turned a real value into NULL did not fit the target type.
            if col.rule == "cast" and "when" not in col.params:
                mask_col = f"__m{len(masks)}"
                work = work.with_columns((pl.col(col.src).is_not_null() & base.is_null()).alias(mask_col))
                masks.append((mask_col, "value_does_not_fit_type", col.dst))

        out_cols.append(col.dst)

    # 2. final cast to the exact target types, flagging values that do not fit
    final_exprs : dict[str, pl.Expr] = {}
    for dst in out_cols:
        target_col = ctx.target_schema[dst]
        casted = pl.col(T + dst).cast(polars_dtype(target_col.pg_type), strict=False)
        final_exprs[dst] = casted

        mask_col = f"__m{len(masks)}"
        work = work.with_columns((pl.col(T + dst).is_not_null() & casted.is_null()).alias(mask_col))
        masks.append((mask_col, "value_does_not_fit_type", dst))

        # NOT NULL columns that the database does not fill itself must have a value
        if not target_col.nullable and not target_col.db_generates_value:
            mask_col = f"__m{len(masks)}"
            work = work.with_columns(casted.is_null().alias(mask_col))
            masks.append((mask_col, "null_in_not_null_column", dst))

    # 3. duplicate primary / unique keys inside this batch (the later duplicates cannot be loaded)
    target_sec = m.raw.get("target") or {}
    key_sets = []
    if target_sec.get("primary_key"):
        key_sets.append(list(target_sec["primary_key"]))
    key_sets.extend(list(k) for k in (target_sec.get("unique") or []))
    for keys in key_sets:
        if not keys or not all(k in out_cols for k in keys):
            continue
        complete = pl.all_horizontal([pl.col(T + k).is_not_null() for k in keys])    # NULLs never collide
        later_duplicate = complete & ~pl.struct([pl.col(T + k) for k in keys]).is_first_distinct()
        mask_col = f"__m{len(masks)}"
        work = work.with_columns(later_duplicate.alias(mask_col))
        masks.append((mask_col, "duplicate_key", ",".join(keys)))

    # 4. split into loadable rows and exceptions
    if masks:
        has_issue = pl.any_horizontal([pl.col(name) for name, _, _ in masks])
        key = _key_expr(work, m)

        parts = []
        for name, reason, column in masks:
            failed = work.filter(pl.col(name))
            if failed.height == 0:
                continue          # a select made only of literals would still return one row
            parts.append(failed.select(
                pl.lit(reason).alias("reason"), pl.lit(column).alias("column"), key.alias("key")))
        exceptions = pl.concat(parts) if parts else _empty_exceptions()
        good = work.filter(~has_issue)
    else:
        exceptions = _empty_exceptions()
        good = work

    rows = good.select([final_exprs[dst].alias(dst) for dst in out_cols])
    return TransformResult(rows=rows, exceptions=exceptions)


if __name__ == "__main__":
    from collections import Counter

    from pipeline.mapping_config import load_mappings
    from pipeline.connections import source_connection, target_connection
    from pipeline.extract import extract_batches, extract_reference_table
    from pipeline.lookups import LookupCache
    from pipeline.target_schema import read_target_schema

    ms = load_mappings()
    masters = [n for n in ms.load_order if "batching" not in ms.tables[n].raw]

    with source_connection() as src, target_connection() as tgt:
        references = {"PredefinedDownCodeInfo": extract_reference_table(src, "PredefinedDownCodeInfo")}
        cache = LookupCache(tgt, ms.tables)

        print(f"{'table':35s} {'in':>5s} {'out':>5s} {'exceptions':>10s}  lookup misses")
        for name in masters:
            tbl = ms.tables[name]
            schema_name = (tbl.raw.get("target") or {}).get("schema", "public")
            ctx = TransformContext(
                load_time=datetime.now(),
                target_schema=read_target_schema(tgt, schema_name, tbl.tgt_table),
                lookups=cache,
                references=references,
            )

            rows_in = rows_out = 0
            reasons : Counter = Counter()
            for batch in extract_batches(src, tbl):
                result = transform_batch(batch, tbl, ctx)
                rows_in += batch.height
                rows_out += result.rows.height
                reasons.update(result.exceptions["reason"].to_list())

            misses = {k: v for k, v in ctx.lookup_misses.items() if v}
            # Only counts and column names are printed, never row values.
            print(f"{name:35s} {rows_in:5d} {rows_out:5d} {sum(reasons.values()):10d}  {misses or ''} {dict(reasons) or ''}")
