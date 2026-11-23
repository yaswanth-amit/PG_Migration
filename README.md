# TPM migration: SQL Server → PostgreSQL

Copies the 25 `master_data` tables and `production_core.autodata` from SQL Server (`TPM_PGSQL`) to PostgreSQL.
One YAML file per table (`config/mappings/`) says where each target column comes from.
It is a plain copy with column mapping and lookups. No business changes.

> Anything that changes a production system needs testing and your normal change-control review first.
> Always run a rehearsal (the default) before a real migration.

*Diagrams are Mermaid. They show on GitHub and in the VS Code markdown preview.*

---

## 1. The pipeline in one picture

```mermaid
flowchart LR
    A[("SQL Server")] -->|"1 EXTRACT<br/>read rows in batches"| B["DataFrame<br/>source columns"]
    B -->|"2 TRANSFORM<br/>map, look up, check"| C["DataFrame<br/>target columns"]
    C -->|"3 LOAD<br/>COPY in a transaction"| D[("PostgreSQL")]
    B -. "bad rows" .-> E["exceptions file<br/>with a reason"]
    C -. "counts" .-> F["migration.* tables<br/>logs, progress"]
```

Every table goes through the same three steps. Tables run in dependency order (parents before children).
A **rehearsal** does everything inside one transaction and rolls it back. A **real** run keeps the data.

---

## 2. Extract

SQL Server rows are read in chunks of 50,000 into a DataFrame. Only the columns the mapping uses are read.

Small tables are read in one go. The big table, `autodata`, is read in **batches of one logical month**.
A *logical day* starts when the first shift starts (for example 06:00), taken from `utility.get_logical_day`.

```
 batch 1: 2026-10-01 06:00  ->  2026-11-01 06:00
 batch 2: 2026-11-01 06:00  ->  2026-12-01 06:00      each batch starts where the last one ended
```

* A row belongs to the batch that contains its start time (`sttime`). No gaps, no overlaps.
* The run stops if the batches miss any row (their total must equal the table's row count).

---

## 3. Transform

Each DataFrame goes through the rules of its YAML file: `copy`, `cast`, `trim`, `coalesce`, `map`, `lookup`, and so on.

* **Lookup:** a business id from the source (for example a machine code) is replaced by the target's `iot_id`,
  using the parent table already loaded in PostgreSQL.
* **Checks:** the value fits the target type, NOT NULL columns have a value, no duplicate key, the parent was found.
* A row that fails a check is **not dropped silently**. It goes to `reports/<table>_exceptions.csv` with a reason.
* After every table: `rows read = rows loaded + rows in the exceptions file`, or the run stops.

### How `is_icd` is solved for autodata

`autodata` has two kinds of rows: **cycles** (`datatype = 1`, a part being machined) and **downtimes** (`datatype = 2`).
A downtime is an **ICD** (in-cycle downtime) when it happens *inside* a cycle that ran longer than its own `cycletime`.

> **Rule:** downtime is ICD when a cycle of the **same machine**, with `duration > cycletime`,
> **starts before** the downtime starts and **ends after** the downtime ends.

SQL Server did this with a slow check per row. Here it is done in Polars. Each batch is split into **three frames**:

```mermaid
flowchart TD
    A["One batch"] --> F1["FRAME 1: cycles that could contain a downtime<br/>cycle longer than its cycletime<br/>read from SQL Server, per machine"]
    A --> F2["FRAME 2: the batch's downtime rows<br/>datatype = 2"]
    A --> F3["FRAME 3: all other rows<br/>normal cycles"]
    F2 --> M{"inside a cycle<br/>of Frame 1?"}
    F1 --> M
    M -->|yes| T["is_icd = true"]
    M -->|no| N["is_icd = false"]
    F3 --> N2["is_icd = false"]
    T --> OUT["batch with is_icd,<br/>goes on to the load"]
    N --> OUT
    N2 --> OUT
```

**Worked example.** Batch 2 starts at 06:00. One cycle began before 06:00 and is still running, so it belongs to batch 1
but is needed here. Machine M1:

```
 06:00 boundary
   |
 00:00 ================== 07:00     cycle C1  (cycletime 1 h, ran 7 h)   <- belongs to batch 1
   |     06:45-06:50                downtime D1
   |        06:55-06:58             downtime D2
   |                07:01-07:04     normal cycle N1
   |                         09:00-09:20  downtime D3
   |                                10:00 ======== 10:30   cycle C2 (cycletime 10 min, ran 30 min)
   |                                           10:10-10:15  downtime D4
```

| Frame | Rows | Notes |
|---|---|---|
| **1: cycles that could contain a downtime** | C1, C2 | C1 is *borrowed* from batch 1. It is used only to check, and is **never loaded again**. C2 belongs to batch 2. |
| **2: downtime rows of batch 2** | D1, D2, D3, D4 | These are the rows that get tested. |
| **3: all other rows of batch 2** | N1 | Nothing to test. |

| Downtime | Inside a Frame 1 cycle? | `is_icd` |
|---|---|---|
| D1 06:45-06:50 | yes, C1 (00:00-07:00) | **true** |
| D2 06:55-06:58 | yes, C1 | **true** |
| D3 09:00-09:20 | no | false |
| D4 10:10-10:15 | yes, C2 (10:00-10:30) | **true** |
| N1 (a cycle) | not tested | false |

Why a *borrowed* cycle is needed: without C1, D1 and D2 would look like ordinary downtimes. A cycle can start in one
batch and its downtimes fall in the next one.

The result equals the original SQL rule. Checked on real data with batches of 7 minutes, 1 hour and 6 hours:
same flags, none missing, none extra.

---

## 4. Load

```mermaid
flowchart LR
    subgraph P["Normal table: one transaction"]
        direction TB
        P1["target must be empty"] --> P2["COPY all batches"] --> P3["record the result"] --> P4(["COMMIT"])
    end
    subgraph W["autodata: one transaction per batch"]
        direction TB
        W1["delete this batch's rows"] --> W2["COPY the batch"] --> W3["record the batch"] --> W4(["COMMIT"])
    end
```

* A table or batch is stored **completely or not at all**. Its result is recorded in the same commit, so the history can
  never say "done" for rows that were rolled back.
* Deleting a batch first makes it **safe to repeat**. Resume skips batches finished earlier, but only if their rows are still in the target.
* If a table fails, the tables that depend on it are skipped. The others continue.
* Counts, times and errors go to `migration.*`, logs and `logs/progress_*.json` (the page reads this live). Never row values.

---

## 5. Run it

```
python main.py list                                    # tables, sources, targets, dependencies
python main.py plan --tables autodata                  # dependency check, no database used
python main.py run  --tables all                       # REHEARSAL (default): runs everything, rolls it back
python main.py run  --tables all --mode real --confirm-target <target database name>
streamlit run ui_app.py                                # the same, with a page
```
Useful options: `--from/--to` (logical days) · `--days N` (fixed windows instead of months) · `--no-resume` ·
`--max-windows 0` (all batches).

**Setup:** `pip install polars pyodbc "psycopg[binary]" pyyaml streamlit pandas`. Passwords come from environment variables
named in `config/connections.yaml`, or a `.env` outside OneDrive (it is in `.gitignore`). Use a dedicated migration login.
A DBA reviews `sql/migration_schema.sql` first. Tests: `python -m pytest -q`.

**Rules:** the target tables must be empty before a real run, and source rows must not change during it.
`autodata_ICD` is not migrated; `is_icd` is derived as in section 3.

**Duplicates:** no unique constraint is added to the target. Rows with the same key (`autodata`: `machine_iot_id` + `st_time`) are
removed from the data before the insert. The first row stays; the others go to the exceptions report as `duplicate_key`.
**Open items:** production-size timing of the per-batch cycle query is not measured yet.
