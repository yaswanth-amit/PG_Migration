from datetime import datetime

import polars as pl

from pipeline import extract
from pipeline.down_aggregate import DOWN_COLUMNS, DownAggregate, aggregate_down_rows
from pipeline.mapping_config import load_mappings

M = load_mappings().tables["shift_production_down_details"]
CFG = M.raw["down_aggregate"]
D = datetime(2026, 11, 3)


def down(**over):
    base = {"dDate": D, "Shift": "Shift-A", "PlantID": "P", "GroupID": "G", "MachineID": "M1", "ComponentID": "C",
            "OperationNo": 10, "OperatorID": "E", "WorkOrderNumber": "WO", "HeatCode": "H", "DownTime": 0, "PDT": 0.0,
            "Threshold": 0, "ML_Flag": 0, "PE_Flag": 0, "prodeffy": 0, "ThresholdfromCO": 0,
            "ShiftStart": datetime(2026, 11, 3, 6), "ShiftEnd": datetime(2026, 11, 3, 14)}
    base.update(over)
    return base


def frame(*rows):
    return pl.DataFrame(list(rows))


def production(**over):
    base = {"pDate": D, "Shift": "Shift-A", "PlantID": "P", "GroupID": "G", "MachineID": "M1", "ComponentID": "C",
            "OperationNo": 10, "OperatorID": "E", "WorkOrderNumber": "WO", "HeatCode": "H", "Prod_Qty": 5.0}
    base.update(over)
    return pl.DataFrame([base])


def agg(*rows):
    return DownAggregate.from_down_rows(CFG, frame(*rows))


def values(df):
    return df.select(DOWN_COLUMNS).row(0, named=True)


def test_the_seven_columns_follow_the_live_formulas():
    sums = agg(
        down(DownTime=695, PDT=0.0, Threshold=180, ML_Flag=1),                       # management loss, capped at 180
        down(DownTime=100, PDT=26.0, Threshold=0, prodeffy=1),                       # operator loss
        down(DownTime=40, PDT=5.0, PE_Flag=1),                                       # minor down (gross 45)
        down(DownTime=0, PDT=126.0),                                                 # only planned
    ).apply(production())
    got = values(sums)
    assert got["net_downtime"] == 835
    assert got["down_planned_downtime"] == 157
    assert got["management_loss"] == 180
    assert got["management_loss_planned_downtime"] == 0           # 695 and 695 are both capped at 180
    assert got["operator_loss"] == 100
    assert got["operator_loss_planned_downtime"] == 26
    assert got["total_minor_downtime"] == 45          # gross: the live procedure does not take the planned part off


def test_the_planned_part_that_the_cap_absorbs_is_management_loss_planned_downtime():
    got = values(agg(down(DownTime=100, PDT=100.0, Threshold=150, ML_Flag=1)).apply(production()))
    # cap(100 + 100) = 150, cap(100) = 100  ->  50
    assert got["management_loss"] == 100
    assert got["management_loss_planned_downtime"] == 50


def test_operator_loss_ignores_rows_whose_threshold_comes_from_the_component():
    got = values(agg(down(DownTime=30, prodeffy=1, ThresholdfromCO=1)).apply(production()))
    assert got["operator_loss"] == 0 and got["net_downtime"] == 30


def test_a_production_row_without_down_rows_gets_zero_and_nothing_is_lost():
    out = agg(down(MachineID="OTHER")).apply(production())
    assert out.height == 1
    assert all(v == 0 for v in values(out).values())


def test_a_null_work_order_or_heat_code_matches_the_zero_text():
    out = agg(down(WorkOrderNumber=None, HeatCode=None, DownTime=9)).apply(production(WorkOrderNumber="0", HeatCode=None))
    assert values(out)["net_downtime"] == 9


def test_a_null_operator_matches_a_null_operator():
    out = agg(down(OperatorID=None, DownTime=7)).apply(production(OperatorID=None))
    assert values(out)["net_downtime"] == 7


def test_keys_with_down_rows_only_become_new_rows_with_zero_production():
    sums = agg(down(DownTime=60, MachineID="M1"), down(DownTime=11, MachineID="M2", ML_Flag=1, Threshold=5))
    sums.apply(production(MachineID="M1"))
    extra = sums.leftover(extract.collect_source_columns(M))
    assert extra.height == 1 and sums.extra_rows == 1
    row = extra.row(0, named=True)
    assert row["MachineID"] == "M2" and row["pDate"] == D and row["Shift"] == "Shift-A"
    assert row["UpdatedBy"] == "migration" and row["Prod_Qty"] is None          # null_as: zero turns it into 0 in the transform
    assert row["ShiftStart"] == datetime(2026, 11, 3, 6)
    assert row["net_downtime"] == 11 and row["management_loss"] == 5


def test_a_down_key_without_a_machine_is_not_made_a_row():
    sums = agg(down(MachineID=None, DownTime=3))
    assert sums.leftover(extract.collect_source_columns(M)).height == 0


def test_nothing_is_left_over_when_every_key_has_a_production_row():
    sums = agg(down())
    sums.apply(production())
    assert sums.leftover(extract.collect_source_columns(M)).height == 0


def test_an_empty_window_has_no_extra_rows_and_zero_sums():
    sums = DownAggregate.empty(CFG)
    out = sums.apply(production())
    assert values(out) == {c: 0 for c in DOWN_COLUMNS}
    assert sums.leftover(["pDate"]).height == 0


def test_the_aggregate_has_one_row_per_key():
    out = aggregate_down_rows(frame(down(), down(DownTime=1), down(MachineID="M2")), list(CFG["key"].values()),
                              ["WorkOrderNumber", "HeatCode"])
    assert out.height == 2


def test_the_flags_come_from_the_row_when_the_down_table_has_them():
    sql = DownAggregate.build_select_sql(CFG, {"ml_flag", "pe_flag", "prodeffy", "thresholdfromco"})
    assert "a.[prodeffy]" in sql and "LEFT JOIN" not in sql and "m." not in sql


def test_the_missing_flags_come_from_the_down_code_master():
    sql = DownAggregate.build_select_sql(CFG, {"ml_flag", "pe_flag"})
    assert "m.[prodeffy] AS [prodeffy]" in sql and "m.[ThresholdfromCO] AS [ThresholdfromCO]" in sql
    assert "LEFT JOIN [dbo].[downcodeinformation] AS m ON a.[DownID] = m.[downid]" in sql
    assert "a.[ML_Flag]" in sql and sql.endswith("WHERE a.[dDate] >= ? AND a.[dDate] < ?")


def test_missing_flags_without_a_master_are_refused():
    cfg = {k: v for k, v in CFG.items() if k != "flag_master"}
    try:
        DownAggregate.build_select_sql(cfg, {"ml_flag", "pe_flag"})
    except ValueError as e:
        assert "flag_master" in str(e)
    else:
        raise AssertionError("missing flags without a master must be refused")
