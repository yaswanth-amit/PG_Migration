"""Which master values do the aggregate tables use that the masters do not have?  (python main.py check-masters)

Read-only. Before the aggregate tables (ShiftProductionDetails, ShiftDownTimeDetails, ShiftRejectionDetails,
ShiftReworkDetails) are migrated, every id they carry (plant, group, machine, component, operation, operator, shift,
down code, rejection code, rework code) is looked up in the master tables. An id the masters do not have cannot get its
iot id, so the row loses it (or is listed as an exception). This check lists those ids, with how many rows use each,
so the masters can be fixed in the SOURCE database first, and the migration then run from scratch.

The comparison is exact (case and spaces count), the same as the lookups of the pipeline. A NULL id is not a missing
master; it is listed as 'empty' so it is not mistaken for one.
"""
import csv
from dataclasses import dataclass
from pathlib import Path

REPORT_DIR = Path(__file__).resolve().parent.parent / "reports"

# (check name, master table, master column, [(aggregate table, column), ...])
SIMPLE_CHECKS = [
    ("plant", "PlantInformation", "PlantID", [("ShiftProductionDetails", "PlantID"), ("ShiftDownTimeDetails", "PlantID")]),
    ("group", "PlantMachineGroups", "GroupID", [("ShiftProductionDetails", "GroupID"), ("ShiftDownTimeDetails", "GroupID")]),
    ("machine", "machineinformation", "machineid", [("ShiftProductionDetails", "MachineID"), ("ShiftDownTimeDetails", "MachineID")]),
    ("component", "componentinformation", "componentid", [("ShiftProductionDetails", "ComponentID"), ("ShiftDownTimeDetails", "ComponentID")]),
    ("operator", "employeeinformation", "Employeeid", [("ShiftProductionDetails", "OperatorID"), ("ShiftDownTimeDetails", "OperatorID")]),
    ("shift", "shiftdetails", "ShiftName", [("ShiftProductionDetails", "Shift"), ("ShiftDownTimeDetails", "Shift")]),
    ("down code", "downcodeinformation", "downid", [("ShiftDownTimeDetails", "DownID")]),
    ("rejection code", "rejectioncodeinformation", "rejectionid", [("ShiftRejectionDetails", "Rejection_Reason")]),
    ("rework code", "Reworkinformation", "Reworkid", [("ShiftReworkDetails", "Rework_Reason")]),
]
# the operation is found by machine + component + operation number (component_operation_pricing)
OPERATION_MASTER = ("componentoperationpricing", ("machineid", "componentid", "operationno"))
OPERATION_USERS = [("ShiftProductionDetails", ("MachineID", "ComponentID", "OperationNo")),
                   ("ShiftDownTimeDetails", ("MachineID", "ComponentID", "OperationNo"))]
PREDEFINED_DOWN_CODES = ("PredefinedDownCodeInfo", "DownId")     # matched by name, ignoring case

OUTPUT_COLUMNS = ["check", "kind", "value", "source_table", "rows"]


@dataclass
class Finding:
    check : str
    kind : str            # 'missing' (not in the masters) or 'empty' (NULL in the aggregate row)
    value : str
    source_table : str
    rows : int

    def as_row(self) -> list:
        return [self.check, self.kind, self.value, self.source_table, self.rows]


def _q(name : str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _fetch(conn , sql : str) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        cur.close()


def _text(value) -> str:
    return "" if value is None else str(value)


def check_masters(conn , progress=lambda message: None) -> list[Finding]:
    """All findings, most-used ids first. `conn` is a SQL Server connection (the source database)."""
    findings : list[Finding] = []
    predefined = {str(r[0]).lower() for r in _fetch(conn, f"SELECT {_q(PREDEFINED_DOWN_CODES[1])} FROM dbo.{_q(PREDEFINED_DOWN_CODES[0])}")
                  if r[0] is not None}

    for name, master_table, master_col, users in SIMPLE_CHECKS:
        progress(f"checking {name} ...")
        known = {str(r[0]) for r in _fetch(conn, f"SELECT DISTINCT {_q(master_col)} FROM dbo.{_q(master_table)} WHERE {_q(master_col)} IS NOT NULL")}
        known_lower = {k.lower() for k in known}
        for table, column in users:
            for value, rows in _fetch(conn, f"SELECT {_q(column)}, COUNT(*) FROM dbo.{_q(table)} GROUP BY {_q(column)}"):
                if value is None:
                    findings.append(Finding(name, "empty", "", table, int(rows)))
                    continue
                text = str(value)
                if text in known:
                    continue
                if name == "down code" and text.lower() in predefined and text.lower() in known_lower:
                    continue                              # a predefined code (NO_DATA, McTI, ...) is matched ignoring case
                findings.append(Finding(name, "missing", text, table, int(rows)))

    progress("checking operation ...")
    master_table, cols = OPERATION_MASTER
    known_ops = {tuple(_text(v) for v in r) for r in _fetch(
        conn, f"SELECT DISTINCT {', '.join(_q(c) for c in cols)} FROM dbo.{_q(master_table)}")}
    machines = {f.value for f in findings if f.check == "machine"}
    components = {f.value for f in findings if f.check == "component"}
    for table, users_cols in OPERATION_USERS:
        sql = f"SELECT {', '.join(_q(c) for c in users_cols)}, COUNT(*) FROM dbo.{_q(table)} GROUP BY {', '.join(_q(c) for c in users_cols)}"
        for machine, component, operation, rows in _fetch(conn, sql):
            if machine is None or component is None or operation is None:
                continue
            if str(machine) in machines or str(component) in components:
                continue                                  # already reported as a missing machine / component
            if (_text(machine), _text(component), _text(operation)) not in known_ops:
                findings.append(Finding("operation", "missing", f"{machine} | {component} | {operation}", table, int(rows)))

    findings.sort(key=lambda f: (f.check, f.kind, -f.rows, f.value))
    return findings


def summarize(findings : list[Finding]) -> list[tuple[str, int, int]]:
    """(check, distinct missing ids, rows affected), for the 'missing' findings only."""
    out : dict[str, list] = {}
    for f in findings:
        if f.kind != "missing":
            continue
        entry = out.setdefault(f.check, [set(), 0])
        entry[0].add(f.value)
        entry[1] += f.rows
    return [(check, len(values), rows) for check, (values, rows) in sorted(out.items())]


def write_report(findings : list[Finding] , path : Path | None = None) -> Path:
    path = path or (REPORT_DIR / "missing_masters.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(OUTPUT_COLUMNS)
        writer.writerows(item.as_row() for item in findings)
    return path
