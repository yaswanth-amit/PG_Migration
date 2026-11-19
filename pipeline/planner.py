"""Decides WHAT can be migrated and in WHICH order. No database and no UI in here, so both the
command line and the Streamlit page use exactly the same rules.
"""
from pipeline.mapping_config import TableMapping, table_dependencies


def validate_selection(selected : set[str] , tables : dict[str, TableMapping] , completed : set[str]) -> list[str]:
    """Return a list of error messages; an empty list means the selection can be run.

    selected  - the tables the user ticked
    completed - tables already migrated successfully in an earlier real run
    A table can run only when every table it depends on is selected too, or already completed.
    """
    errors : list[str] = []

    if not selected:
        return ["Nothing is selected."]

    unknown = sorted(selected - set(tables))
    for name in unknown:
        errors.append(f"'{name}' is not a mapped table.")

    for name in sorted(selected & set(tables)):
        missing = [dep for dep in sorted(table_dependencies(tables[name]))
                   if dep not in selected and dep not in completed]
        if missing:
            errors.append(f"'{name}' cannot be migrated yet: it needs {', '.join(missing)} "
                          f"(not selected and not migrated yet).")

    return errors


def with_dependencies(selected : set[str] , tables : dict[str, TableMapping] , completed : set[str]) -> set[str]:
    """The selection plus every table it still needs (for an 'add required tables' button)."""
    result = set(selected)
    pending = list(selected)

    while pending:
        name = pending.pop()
        if name not in tables:
            continue
        for dep in table_dependencies(tables[name]):
            if dep not in result and dep not in completed:
                result.add(dep)
                pending.append(dep)

    return result


def plan_run(selected : set[str] , load_order : list[str]) -> list[str]:
    """The selected tables in dependency order."""
    return [name for name in load_order if name in selected]


def dependents(name : str , tables : dict[str, TableMapping]) -> list[str]:
    """Tables that need `name` (shown in the UI so people see what a table unlocks)."""
    return sorted(other for other, m in tables.items() if name in table_dependencies(m))
