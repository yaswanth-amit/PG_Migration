"""Tests for planner.py using the real mapping files (no database needed).
Run with:  python test_planner.py     (or: pytest test_planner.py)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))      # the project folder

from pipeline.mapping_config import load_mappings
from pipeline.planner import validate_selection, with_dependencies, plan_run, dependents

MS = load_mappings()
TABLES = MS.tables
COP = "component_operation_pricing"


def test_select_all_is_valid_and_ordered_by_dependency():
    everything = set(TABLES)
    assert validate_selection(everything, TABLES, completed=set()) == []
    order = plan_run(everything, MS.load_order)
    assert order == MS.load_order
    assert order.index("machine_information") < order.index(COP)
    assert order.index("component_information") < order.index(COP)


def test_cop_alone_is_refused_and_names_the_missing_tables():
    errors = validate_selection({COP}, TABLES, completed=set())
    assert len(errors) == 1
    assert COP in errors[0] and "machine_information" in errors[0] and "component_information" in errors[0]


def test_cop_is_allowed_when_its_parents_were_migrated_earlier():
    done = {"machine_information", "component_information"}
    assert validate_selection({COP}, TABLES, completed=done) == []


def test_cop_with_only_one_parent_names_the_other():
    errors = validate_selection({COP, "machine_information"}, TABLES, completed=set())
    assert len(errors) == 1                      # machine_information is selected, so only component_information is missing
    assert COP in errors[0] and "component_information" in errors[0]
    assert "machine_information" not in errors[0]


def test_add_required_tables_follows_the_chain():
    needed = with_dependencies({COP}, TABLES, completed=set())
    assert {"machine_information", "component_information", "customer_information"} <= needed
    assert validate_selection(needed, TABLES, completed=set()) == []


def test_add_required_tables_skips_what_is_already_migrated():
    needed = with_dependencies({COP}, TABLES, completed={"customer_information", "machine_information"})
    assert needed == {COP, "component_information"}


def test_empty_and_unknown_selections():
    assert validate_selection(set(), TABLES, set()) == ["Nothing is selected."]
    assert any("not a mapped table" in e for e in validate_selection({"nope"}, TABLES, set()))


def test_autodata_needs_all_its_lookup_tables():
    errors = validate_selection({"autodata"}, TABLES, completed=set())
    assert len(errors) == 1
    for parent in ("machine_information", "component_information", COP, "employee_information", "down_code_information"):
        assert parent in errors[0]


def test_dependents_show_what_a_table_unlocks():
    assert COP in dependents("machine_information", TABLES)
    assert "autodata" in dependents("machine_information", TABLES)


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok    {name}")
        except Exception as e:                    # noqa: BLE001 - a test runner reports every kind of failure
            failed += 1
            print(f"  FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    raise SystemExit(1 if failed else 0)
