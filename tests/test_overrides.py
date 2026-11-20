from pipeline import mapping_overrides as mo
from pipeline.mapping_config import ColumnRule, TableMapping, load_mappings
from pipeline.target_schema import TargetColumn


def make_table() -> TableMapping:
    cols = [ColumnRule("plant_id", "PlantID", "copy"), ColumnRule("name", "Name", "trim")]
    return TableMapping("t", "T", "t", [], cols, {}, ["Old"], {"source": {"schema": "dbo"}, "target": {"schema": "s"}})


SRC = [{"name": "PlantID", "type": "nvarchar", "type_text": "nvarchar(10)", "nullable": False},
       {"name": "Name", "type": "nvarchar", "type_text": "nvarchar(50)", "nullable": True},
       {"name": "Old", "type": "int", "type_text": "int", "nullable": True},
       {"name": "Extra", "type": "int", "type_text": "int", "nullable": True},
       {"name": "Note", "type": "nvarchar", "type_text": "nvarchar(50)", "nullable": True}]
TGT = {"plant_id": TargetColumn("plant_id", "text", False),
       "name": TargetColumn("name", "text", True),
       "qty": TargetColumn("qty", "integer", True),
       "note": TargetColumn("note", "text", False),
       "updated_at": TargetColumn("updated_at", "timestamp without time zone", True),
       "iot_id": TargetColumn("iot_id", "bigint", False, "nextval('x'::regclass)")}


def test_compatibility():
    assert mo.compatibility("int", "integer")[0] == "ok"
    assert mo.compatibility("bigint", "smallint")[0] == "cast"
    assert mo.compatibility("bit", "boolean") == ("ok", "bool", None)
    assert mo.compatibility("nvarchar", "integer")[0] == "cast"
    assert mo.compatibility("varbinary", "integer")[0] == "blocked"


def test_classify_source_statuses():
    rows = {r["name"]: r["status"] for r in mo.classify_source(make_table(), SRC)}
    assert rows == {"PlantID": mo.MAPPED, "Name": mo.MAPPED, "Old": mo.NOT_MIGRATED,
                    "Extra": mo.UNDECIDED, "Note": mo.UNDECIDED}


def test_classify_target_statuses():
    rows = {r["name"]: r["status"] for r in mo.classify_target(make_table(), TGT)}
    assert rows["plant_id"] == "mapped" and rows["qty"] == "no value yet"
    assert rows["iot_id"] == "database generates"


def test_reviewed_column_cannot_be_changed():
    data = {"columns": [{"dst": "plant_id", "rule": "constant", "value": "x"}]}
    assert any("already mapped" in e for e in mo.validate_override(make_table(), data, SRC, TGT))


def test_source_used_by_reviewed_mapping_is_rejected():
    data = {"columns": [{"dst": "qty", "src": "PlantID", "rule": "cast", "to": "integer"}]}
    assert any("already used" in e for e in mo.validate_override(make_table(), data, SRC, TGT))


def test_good_choices_are_accepted():
    data = {"columns": [{"dst": "qty", "src": "Extra", "rule": "copy"},
                        {"dst": "updated_at", "rule": "load_time"}],
            "leave_unmapped": ["Note"]}
    assert mo.validate_override(make_table(), data, SRC, TGT) == []


def test_not_null_rules():
    data = {"columns": [{"dst": "note", "rule": "set_null"}]}
    assert any("NOT NULL" in e for e in mo.validate_override(make_table(), data, SRC, TGT))
    data = {"columns": [{"dst": "note", "src": "Note", "rule": "copy"}]}
    assert any("coalesce" in e for e in mo.validate_override(make_table(), data, SRC, TGT))
    data = {"columns": [{"dst": "note", "src": "Note", "rule": "coalesce", "fallback": "n/a"}]}
    assert mo.validate_override(make_table(), data, SRC, TGT) == []


def test_generated_column_and_cast_needed():
    assert any("generates" in e for e in mo.validate_override(
        make_table(), {"columns": [{"dst": "iot_id", "rule": "load_time"}]}, SRC, TGT))
    data = {"columns": [{"dst": "qty", "src": "Note", "rule": "copy"}]}
    assert any("needs a cast" in e for e in mo.validate_override(make_table(), data, SRC, TGT))


def test_apply_and_file_round_trip(tmp_path):
    m = make_table()
    data = {"columns": [{"dst": "updated_at", "rule": "load_time"}], "leave_unmapped": []}
    mo.save_override("t", data, "tester", tmp_path)
    assert mo.load_override("t", tmp_path)["columns"] == data["columns"]
    errors = mo.apply_overrides({"t": m}, tmp_path)
    assert errors == [] and m.columns[-1].dst == "updated_at" and m.columns[-1].rule == "load_time"
    mo.save_override("t", {}, "tester", tmp_path)            # reset removes the file
    assert not mo.override_path("t", tmp_path).exists()


def test_real_mappings_load_with_and_without_choices(tmp_path):
    base = load_mappings(overrides_dir=tmp_path)             # empty folder = no choices
    again = mo.load_reviewed()
    assert base.load_order == again.load_order
