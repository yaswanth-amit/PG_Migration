from dataclasses import dataclass, field
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent      # the project folder
MAPPING_DIR = BASE_DIR / "config" / "mappings"

# Every rule the mapping files may use, with the parameters each one must have.
RULES: dict[str, set[str]] = {
    "copy": set(),
    "cast": {"to"},
    "lookup": {"table", "match", "return"},
    "coalesce": set(),              # needs 'fallback' OR 'fallback_col' (checked separately)
    "bool": set(),
    "bool_from_text": {"true_values", "false_values"},
    "constant": {"value"},
    "trim": set(),
    "map": {"values"},
    "max": set(),
    "first_non_null": set(),
    "exists_in": {"table", "column"},
    "load_time": set(),
    "omit": set(),
    "set_null": set(),
    "derived": {"name"},            # computed from several columns, described under derived_rules in the YAML
    "shift_time": {"part", "date", "shift"},   # the source value, or the shift's start / end on that date (from shift_details)
}

# Rules that read a source column, so 'src' must be given.
RULES_NEEDING_SRC = {
    "copy", "cast", "bool", "bool_from_text", "trim", "map",
    "max", "first_non_null", "coalesce", "exists_in", "shift_time",
}


@dataclass
class ColumnRule:
    dst : str | None
    src : str | None
    rule : str | None
    params : dict = field(default_factory=dict)

@dataclass
class TableMapping:
    name : str
    src_table : str
    tgt_table : str
    depends_on : list[str]
    columns : list[ColumnRule]
    default : dict
    unmapped_src : list[str]
    raw : dict


@dataclass
class MappingSet:
    tables : dict[str,TableMapping]
    load_order : list[str]


def load_yaml_file(mapping_file : Path) -> dict:
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

    try:
        with open(mapping_file,"r",encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {mapping_file.name}: {e}") from e

    return data or {}


def parse_table(data : dict , file_name : str) -> TableMapping:
    src_sec = data.get('source') or {}
    tgt_sec = data.get('target') or {}

    # Table name defaults to target table or file stem
    tbl_name = tgt_sec.get("table") or Path(file_name).stem
    src_table = src_sec.get("table","")
    tgt_table = tgt_sec.get("table","")
    depends_on = data.get('depends_on') or []

    # Parse columns into ColumnRule instance
    columns : list[ColumnRule] = []

    for col in data.get("columns") or []:
        dst = col.get("dst")
        src = col.get("src")
        rule = col.get("rule","copy")      # only a missing key defaults to copy

        params = {k:v for k,v in col.items() if k not in ("dst","src","rule")}

        columns.append(ColumnRule(dst=dst,src=src,rule=rule,params=params))

    return TableMapping(
        name=tbl_name,
        src_table=src_table,
        tgt_table=tgt_table,
        depends_on=depends_on,
        columns=columns,
        default=data.get("defaults") or {},
        unmapped_src=data.get("unmapped_source") or [],
        raw=data,
    )


def table_dependencies(m : TableMapping) -> set[str]:
    """Tables that must be loaded before m: the declared depends_on plus every lookup table."""
    deps = set(m.depends_on)
    for col in m.columns:
        if col.rule == "lookup" and col.params.get("table"):
            deps.add(col.params["table"])
    deps.discard(m.name)       # a table may look itself up; that is not a load-order dependency
    return deps


def validate_mapping(m : TableMapping , known_tables : set[str]) -> list[str]:
    errors : list[str] = []
    if not m.src_table:
        errors.append(f"Table '{m.name}': missing source table name.")
    if not m.tgt_table:
        errors.append(f"Table '{m.name}': missing target table name.")

    for dep in m.depends_on:
        if dep not in known_tables:
            errors.append(f"Table '{m.name}': dependency '{dep}' is not among known tables.")

    seen_dst : set[str] = set()
    for i, col in enumerate(m.columns, start=1):
        where = f"Table '{m.name}', column #{i} ('{col.dst}')"

        if not col.dst:
            errors.append(f"Table '{m.name}', column #{i}: missing 'dst'.")
        elif col.dst in seen_dst:
            errors.append(f"{where}: duplicate target column.")
        else:
            seen_dst.add(col.dst)

        if col.rule not in RULES:
            errors.append(f"{where}: unknown rule '{col.rule}'.")
            continue

        missing = RULES[col.rule] - col.params.keys()
        if missing:
            errors.append(f"{where}: rule '{col.rule}' is missing parameter(s): {sorted(missing)}.")

        if col.rule == "coalesce" and not ({"fallback", "fallback_col"} & col.params.keys()):
            errors.append(f"{where}: rule 'coalesce' needs 'fallback' or 'fallback_col'.")

        if col.rule in RULES_NEEDING_SRC and not col.src:
            errors.append(f"{where}: rule '{col.rule}' needs a 'src' column.")

        if col.rule == "lookup" and col.params.get("table") not in known_tables:
            errors.append(f"{where}: lookup table '{col.params.get('table')}' is not among known tables.")

        if col.rule == "lookup" and col.params.get("on_miss", "null") not in ("null", "drop"):
            errors.append(f"{where}: on_miss must be 'null' or 'drop', not '{col.params.get('on_miss')}'.")

        if col.rule == "lookup" and col.params.get("on_miss") == "drop" and "predefined_match" in col.params:
            errors.append(f"{where}: on_miss: drop cannot be combined with predefined_match.")

    return errors


def build_load_order(tables : dict[str,TableMapping]) -> list[str]:
    # in_degree: number of dependencies remaining for each table

    in_degree = {name : 0 for name in tables}
    graph = {name : [] for name in tables}

    for name , tbl in tables.items():
        for dep in table_dependencies(tbl):
            if dep in tables:
                graph[dep].append(name)
                in_degree[name] += 1

    queue = sorted(name for name,deg in in_degree.items() if deg == 0)
    load_order: list[str] = []

    while queue:
        curr = queue.pop(0)
        load_order.append(curr)
        for dependent in sorted(graph[curr]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(load_order) != len(tables):
        circular = [t for t in tables if t not in load_order]
        raise ValueError(f"Circular dependency detected among tables: {circular}")

    return load_order


def load_mappings(folder : Path = MAPPING_DIR , overrides_dir : Path | None = None) -> MappingSet:
    """overrides_dir: where the user's gap-filling choices are (default: config/overrides when folder is the
    standard mapping folder, otherwise none). Choices never change a reviewed mapping, they only add to it."""
    tables : dict[str,TableMapping] = {}
    all_errors : list[str] = []

    # Find all yaml files in a fixed order (skip files starting with '_', e.g. _conventions.yaml)
    for yml_file in sorted(folder.rglob("*.yaml")):
        if yml_file.name.startswith("_"):
            continue

        data = load_yaml_file(yml_file)

        if "source" not in data or "target" not in data:
            all_errors.append(f"{yml_file.name}: must have both a 'source' and a 'target' section.")
            continue

        tbl = parse_table(data,yml_file.name)

        if tbl.name in tables:
            all_errors.append(f"{yml_file.name}: duplicate table name '{tbl.name}'.")
            continue

        tables[tbl.name] = tbl

    if not tables and not all_errors:
        all_errors.append(f"No mapping files found under {folder}")

    # Validate once, after every file is loaded, and report all problems together
    known_tables = set(tables.keys())
    for tbl in tables.values():
        all_errors.extend(validate_mapping(tbl, known_tables))

    if not all_errors:
        from pipeline import mapping_overrides
        if overrides_dir is None and folder == MAPPING_DIR:
            overrides_dir = mapping_overrides.OVERRIDE_DIR
        if overrides_dir is not None:
            all_errors.extend(mapping_overrides.apply_overrides(tables, overrides_dir))

    if all_errors:
        raise ValueError("Validation failed:\n" + "\n".join(all_errors))

    load_order = build_load_order(tables)

    return MappingSet(tables=tables,load_order=load_order)


if __name__ == "__main__":
    mapping_set = load_mappings()
    print(f"{len(mapping_set.tables)} tables loaded")
    print("Load order:")
    for position, name in enumerate(mapping_set.load_order, start=1):
        print(f"  {position:2d}. {name}")
