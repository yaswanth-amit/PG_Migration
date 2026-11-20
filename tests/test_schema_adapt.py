from pipeline.mapping_config import ColumnRule, TableMapping
from pipeline.schema_adapt import adapt_to_source, list_source_columns
from pipeline.target_schema import TargetColumn


def table() -> TableMapping:
    cols = [ColumnRule("id", "ID", "copy"),
            ColumnRule("updated_ts", "UpdatedTS", "copy"),
            ColumnRule("landing", "LandingPage", "copy"),
            ColumnRule("name", "Name", "coalesce", {"fallback_col": "AltName"}),
            ColumnRule("created", None, "load_time")]
    return TableMapping("t", "T", "t", [], cols, {}, [], {"source": {"schema": "dbo"}})


TGT = {"id": TargetColumn("id", "text", False), "updated_ts": TargetColumn("updated_ts", "timestamp", True),
       "landing": TargetColumn("landing", "text", True), "name": TargetColumn("name", "text", True),
       "created": TargetColumn("created", "timestamp", True)}


def test_missing_columns_are_left_out_and_reported():
    present = {"id", "name"}
    m, a = adapt_to_source(table(), present, TGT)
    assert [c.dst for c in m.columns] == ["id", "name", "created"]
    assert {d["dst"] for d in a.dropped} == {"updated_ts", "landing"}
    assert a.blockers == []
    assert "fallback_col" not in m.columns[1].params          # missing optional fallback is not used


def test_original_mapping_is_not_changed():
    original = table()
    adapt_to_source(original, {"id"}, TGT)
    assert len(original.columns) == 5


def test_nothing_missing_returns_same_mapping():
    m = table()
    same, a = adapt_to_source(m, {"id", "updatedts", "landingpage", "name", "altname"}, TGT)
    assert same is m and a.dropped == []


def test_not_null_target_without_default_blocks_the_table():
    present = {"name", "altname"}                            # ID is missing and target 'id' is NOT NULL
    _, a = adapt_to_source(table(), present, TGT)
    assert a.blockers and "id" in a.blockers[0]


def test_unreadable_source_columns_means_no_change():
    m = table()
    assert adapt_to_source(m, None, TGT)[0] is m


def test_list_source_columns_handles_errors():
    class Bad:
        def cursor(self):
            raise RuntimeError("no")
    assert list_source_columns(Bad(), table()) is None
