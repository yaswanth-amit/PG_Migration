-- Tracking schema for the SQL Server -> PostgreSQL migration.
-- STATUS: DRAFT FOR REVIEW. Not executed. A DBA must review and run it on a test database first,
-- then through the normal change-control process.
-- The tracking tables hold counts, times and natural keys only. They never hold row values.

CREATE SCHEMA IF NOT EXISTS migration;

-- One row per migration run (a click on "Migrate", or one command-line call).
CREATE TABLE IF NOT EXISTS migration.run (
    run_id        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    mode          text        NOT NULL CHECK (mode IN ('rehearsal', 'real')),
    status        text        NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'completed', 'completed_with_exceptions', 'failed', 'cancelled')),
    started_by    text,                      -- operating-system user name
    source_db     text,                      -- for example TPM_PGSQL (no passwords, no connection strings)
    target_db     text,
    tables_requested text[]    NOT NULL DEFAULT '{}',
    note          text
);

-- One row per table in a run: how many rows were read, loaded and held back.
CREATE TABLE IF NOT EXISTS migration.table_run (
    id               bigserial   PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES migration.run(run_id) ON DELETE CASCADE,
    table_name       text        NOT NULL,   -- mapping name, for example customer_information
    target_table     text        NOT NULL,   -- schema.table
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'completed', 'completed_with_exceptions', 'failed', 'blocked', 'skipped')),
    rows_read        bigint      NOT NULL DEFAULT 0,
    rows_loaded      bigint      NOT NULL DEFAULT 0,
    rows_excepted    bigint      NOT NULL DEFAULT 0,
    lookup_misses    jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- {"component_information.customer_iot_id": 6}
    started_at       timestamptz,
    finished_at      timestamptz,
    error            text,                    -- error message only, never row values
    exceptions_file  text,
    UNIQUE (run_id, table_name)
);

-- One row per time window of a large table (autodata): the 15-day batches. Makes a run resumable.
CREATE TABLE IF NOT EXISTS migration.window_run (
    id               bigserial   PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES migration.run(run_id) ON DELETE CASCADE,
    table_name       text        NOT NULL,
    window_no        integer     NOT NULL,
    window_start     timestamp,               -- inclusive; NULL together with window_end = the "time is NULL" window
    window_end       timestamp,               -- exclusive
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'completed', 'completed_with_exceptions', 'failed', 'skipped')),
    rows_source      bigint,                  -- COUNT(*) on the source for this window
    rows_read        bigint      NOT NULL DEFAULT 0,
    rows_loaded      bigint      NOT NULL DEFAULT 0,
    rows_excepted    bigint      NOT NULL DEFAULT 0,
    started_at       timestamptz,
    finished_at      timestamptz,
    error            text,
    UNIQUE (run_id, table_name, window_no)
);

-- Why rows could not be loaded, counted per reason (full details are in the exceptions CSV file).
CREATE TABLE IF NOT EXISTS migration.exception_summary (
    id           bigserial PRIMARY KEY,
    run_id       uuid    NOT NULL REFERENCES migration.run(run_id) ON DELETE CASCADE,
    table_name   text    NOT NULL,
    window_no    integer,
    reason       text    NOT NULL,            -- for example null_in_not_null_column
    column_name  text,
    row_count    bigint  NOT NULL
);

-- Major events, for the log panel in the UI (the detailed log is the log file).
CREATE TABLE IF NOT EXISTS migration.event (
    id          bigserial   PRIMARY KEY,
    run_id      uuid        REFERENCES migration.run(run_id) ON DELETE CASCADE,
    at          timestamptz NOT NULL DEFAULT now(),
    level       text        NOT NULL CHECK (level IN ('INFO', 'WARNING', 'ERROR')),
    table_name  text,
    message     text        NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_table_run_table  ON migration.table_run (table_name, finished_at DESC);
CREATE INDEX IF NOT EXISTS ix_window_run_table ON migration.window_run (table_name, window_start);
CREATE INDEX IF NOT EXISTS ix_event_run        ON migration.event (run_id, at);

-- What the UI uses to decide "is this table already migrated?": the latest REAL run that completed.
CREATE OR REPLACE VIEW migration.table_status AS
SELECT DISTINCT ON (t.table_name)
       t.table_name,
       t.status,
       t.rows_read, t.rows_loaded, t.rows_excepted,
       t.finished_at,
       r.run_id, r.mode
FROM migration.table_run t
JOIN migration.run r USING (run_id)
WHERE r.mode = 'real'
  AND t.status IN ('completed', 'completed_with_exceptions', 'failed')
ORDER BY t.table_name, t.finished_at DESC NULLS LAST;
