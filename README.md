# TPM migration: SQL Server → PostgreSQL

Copies the 25 `master_data` tables and `production_core.autodata` from `TPM_PGSQL` (SQL Server) to
PostgreSQL, using one YAML mapping per table. Level 1: a copy with column mapping, no business changes.

**Anything that changes a production system needs testing and your normal change-control review first.**
Always run a rehearsal before a real migration.

## What is where

| Path | What it is |
|---|---|
| `config/mappings/master_data/*.yaml` | one mapping per master table; `_conventions.yaml` holds the shared rules |
| `config/mappings/production_core/autodata.yaml` | the autodata mapping (15-day windows, `is_icd`) |
| `config/connections.yaml` | hosts and database names only. **No passwords.** |
| `sql/migration_schema.sql` | tracking schema (`migration.*`). **A DBA must review and run it.** |
| `main.py` · `ui_app.py` | the two entry points: command line · web page (both stay at the top level) |
| `pipeline/` | all the code: `mapping_config` `extract` `transform` `load` `lookups` `target_schema` (the pipeline), `runner` (orchestration), `planner` `tracking` `log_setup` `services` `connections` |
| `tests/` | the offline tests, one file per module, plus `conftest.py` |
| `logs/` · `reports/` | one log file per run · exception CSVs (reason, column, natural key only); created at the top level |

## One-time setup
1. `pip install polars pyodbc "psycopg[binary]" pyyaml streamlit pandas`
2. Passwords live in environment variables named in `config/connections.yaml` (`user_env`, `password_env`),
   or in a `.env` file **outside OneDrive** whose path is in `PG_MIGRATION_ENV`. Never commit or sync a `.env`.
3. Use a **dedicated migration login**, not `postgres`/`sa`: read-only on the source; on the target it needs
   INSERT/DELETE on the migrated tables and the `migration` schema.
4. Have the DBA review `sql/migration_schema.sql`, then run it on the **test** database first.

## Daily use
```
python main.py list                                    # tables, sources, targets, dependencies
python main.py plan --tables component_operation_pricing   # shows the dependency error, no database used
python main.py run  --tables all                       # REHEARSAL (default): runs everything, rolls it back
python main.py run  --tables all --mode real --confirm-target <target database name>
python main.py run  --tables autodata --from 2026-01-01 --to 2026-03-31 --days 15 --max-windows 0
streamlit run ui_app.py                                # the same, with a page
```
* A table can only run when the tables it depends on are selected too, or already migrated.
* A rehearsal runs the whole chain in **one transaction** and rolls it back, so child tables see the parents
  loaded just before them. It proves the database accepts every row. (Sequences used for generated ids still
  advance: harmless gaps.) For autodata a rehearsal uses the first 2 windows unless `--max-windows 0`.
* A real run commits table by table, and window by window for autodata. A failed table blocks the tables that
  depend on it; independent tables continue. An interrupted autodata run **resumes**: finished windows are skipped, but only when their rows are still in the
  target. If the target was emptied since, those windows are loaded again.
* Exit codes: 0 finished · 1 a table failed · 2 bad request · 3 tracking schema missing.

## Reading the result
* `completed_with_exceptions`: some rows could not be stored (NOT NULL, duplicate key, value too big). They are
  listed in `reports/<table>_exceptions.csv`; nothing is silently dropped.
* **Lookup misses** (for example `component_information.customer_iot_id = 6`) mean a business id has no match in
  its parent table. The row is still loaded with NULL in the link column. This is a data question for the owner.
* The `migration.*` tables hold counts, times and error classes, never row values. Logs never contain row values
  and mask passwords.

## Rules the tool relies on (decided with the owner)
* Target `master_data` tables and `production_core.autodata` are **empty** when a real run starts; nothing else
  writes to `production_core.autodata` during the run (a window is redone by deleting its `st_time` range first).
* Source rows are not modified once the migration starts.
* NULL in the source becomes the column's database default if it has one, otherwise stays NULL; `updated_by` NULL
  becomes `pct`; surrogate `id` columns come from sequences; `autodata_ICD` is not migrated (`is_icd` is computed).
* `is_icd` is evaluated against the whole source table with a bounded lookback, so a cycle in an earlier window is
  found. Verified equal to the original rule for window sizes of 1, 3, 15 and 4000 days.

## Volume test data (source side)
`python -m testdata.synthetic_autodata --rows 10000000` generates 1 crore autodata rows in the SQL Server source that follow the
patterns of the real rows: production cycles, McTI cycles, ICD (also in `autodata_icd`), background downtimes and NO_DATA gaps.
It is a **dry run** unless `--execute` is given; `--cleanup --execute` removes exactly the rows it inserted (tag `SYNTH_1CR` in
`SplString2` / `Remarks`, neither is migrated). New rows are placed before the first real row, so real data and
`autodata_maxtime` are untouched. Use it only on a test database.

## Tests (no database needed)
Run everything from the project folder: `python -m pytest -q`  ·  or one file: `python tests/test_transform.py`
(`test_transform` `test_load` `test_tracking` `test_planner` `test_extract` `test_runner` `test_main` `test_ui`).

Module self-checks that read the real databases (read-only) are run as modules, not as scripts:
`python -m pipeline.extract` (row counts of all masters) · `python -m pipeline.lookups` (lookup preflight) ·
`python -m pipeline.transform` (transform every master) · `python -m pipeline.target_schema` · `python -m pipeline.mapping_config`.

Code inside `pipeline/` imports its neighbours as `from pipeline.<module> import ...`, so it always has to be
started from the project folder (where `main.py` is), never from inside `pipeline/`.

## Known limits
* `load.py` uses row-by-row `COPY` (`write_row`). Correct, and fine for the masters; measure it on a full-size
  autodata window before committing to a schedule, and switch to a faster bulk path if needed.
* The first real run needs the `migration` schema and a write-capable login; neither was available for testing
  tracking against a live database. Everything else was rehearsed against the local `tpm_migration` target.
