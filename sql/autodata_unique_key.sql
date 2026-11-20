-- FOR REVIEW BY THE DBA. NOT RUN BY THE MIGRATION TOOL.
-- Makes (machine_iot_id, st_time) unique on production_core.autodata.
--
-- Why: the migration loads in batches and re-reads a few rows of the previous batch as context only. A unique key
-- makes sure such a row can never be stored twice. The source already guarantees it (IX_MCSTTIME on (mc, sttime)
-- is unique in SQL Server), so migrated data satisfies it.
--
-- The existing non-unique index idx_autodata_mc_st_time on the same columns becomes the unique one: no extra index.
--
-- Before running it on any database that the live Postgres ETL writes to:
--   * test it with the ETL: etl.util_upsert_to_autodata inserts without ON CONFLICT, so a duplicate (machine, st_time)
--     would raise an error and stop the ETL batch instead of being stored twice;
--   * check that no duplicates exist:
--       SELECT machine_iot_id, st_time, count(*) FROM production_core.autodata
--       GROUP BY 1, 2 HAVING count(*) > 1;            -- must return no rows
--
-- CONCURRENTLY cannot run inside a transaction block.

CREATE UNIQUE INDEX CONCURRENTLY uq_autodata_mc_st_time
    ON production_core.autodata (machine_iot_id, st_time);

ALTER TABLE production_core.autodata
    ADD CONSTRAINT uq_autodata_mc_st_time UNIQUE USING INDEX uq_autodata_mc_st_time;

DROP INDEX production_core.idx_autodata_mc_st_time;      -- replaced by the unique one
