from pipeline import master_check as mc


class FakeConn:
    """Answers each query by what it selects, from a dict of {(table, column(s)): rows}."""
    def __init__(self, data): self.data = data
    def cursor(self): return Cur(self.data)


class Cur:
    def __init__(self, data): self.data, self.rows = data, []
    def execute(self, sql):
        for key, rows in self.data.items():
            if all(part in sql for part in key):
                self.rows = rows
                return
        self.rows = []
    def fetchall(self): return self.rows
    def close(self): pass


def conn():
    data = {
        ("PredefinedDownCodeInfo",): [("NO_DATA",)],
        ("FROM dbo.[PlantMachineGroups]",): [("LINE 2",), ("LINE 3",)],
        ("SELECT [GroupID], COUNT(*) FROM dbo.[ShiftProductionDetails]",): [("LINE 1", 10), ("LINE 2", 5), (None, 3)],
        ("SELECT [GroupID], COUNT(*) FROM dbo.[ShiftDownTimeDetails]",): [("LINE 1", 7), ("line 3", 2)],
        ("FROM dbo.[downcodeinformation]",): [("no_data",), ("Inspection",)],
        ("SELECT [DownID], COUNT(*)",): [("NO_DATA", 4), ("Inspection", 1), ("Gone", 2)],
        ("FROM dbo.[componentoperationpricing]",): [("M1", "C1", 10)],
        ("SELECT [MachineID], [ComponentID], [OperationNo], COUNT(*) FROM dbo.[ShiftProductionDetails]",): [("M1", "C1", 10, 9), ("M1", "C1", 20, 4)],
    }
    return FakeConn(data)


def test_missing_group_names_are_found_with_their_row_counts_and_nulls_are_listed_as_empty():
    f = mc.check_masters(conn())
    groups = [(x.kind, x.value, x.source_table, x.rows) for x in f if x.check == "group"]
    assert ("missing", "LINE 1", "ShiftProductionDetails", 10) in groups
    assert ("missing", "LINE 1", "ShiftDownTimeDetails", 7) in groups
    assert ("missing", "line 3", "ShiftDownTimeDetails", 2) in groups          # the comparison is exact: case counts
    assert ("empty", "", "ShiftProductionDetails", 3) in groups
    assert not any(x.value == "LINE 2" for x in f)


def test_a_predefined_down_code_matches_ignoring_case_but_another_code_does_not():
    f = [x for x in mc.check_masters(conn()) if x.check == "down code"]
    assert [x.value for x in f] == ["Gone"]


def test_an_operation_missing_from_the_masters_is_listed():
    f = [x for x in mc.check_masters(conn()) if x.check == "operation"]
    assert [(x.value, x.rows) for x in f] == [("M1 | C1 | 20", 4)]


def test_summary_and_report(tmp_path):
    f = mc.check_masters(conn())
    s = {check: (ids, rows) for check, ids, rows in mc.summarize(f)}
    assert s["group"] == (2, 19) and s["down code"] == (1, 2)
    path = mc.write_report(f, tmp_path / "m.csv")
    text = path.read_text(encoding="utf-8-sig")
    assert text.splitlines()[0] == "check,kind,value,source_table,rows" and "LINE 1" in text
