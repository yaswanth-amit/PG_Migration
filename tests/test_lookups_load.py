from pipeline.lookups import LookupCache, LookupUse
from pipeline.mapping_config import ColumnRule, TableMapping
from pipeline import lookups
from unittest import mock


def test_a_lookup_that_returns_its_own_key_has_one_column():
    """down_id -> down_id (match on the id, return the master's spelling) must not select the column twice."""
    from pipeline.target_schema import TargetColumn
    m = TableMapping("down_code_information", "S", "down_code_information", [], [], {}, [], {"target": {"schema": "master_data"}})
    seen = {}

    class Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, query): seen["query"] = query.as_string(None) if hasattr(query, "as_string") else str(query)
        def fetchall(self): return [("no_data",), ("McTI",)]

    class Conn:
        def cursor(self): return Cur()

    schema = {"down_id": TargetColumn("down_id", "text", False)}
    with mock.patch.object(lookups, "read_target_schema", lambda *a: schema), \
         mock.patch.object(lookups.sql.Composed, "as_string", lambda self, ctx: "SQL " + repr(self), create=True):
        cache = LookupCache(Conn(), {"down_code_information": m})
        frame = cache.get(LookupUse(table="down_code_information", keys=("down_id",), ret="down_id"))
    assert frame.columns == ["down_id"] and frame.height == 2
    assert seen["query"].count("down_id") == 1
