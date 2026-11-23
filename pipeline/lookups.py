from dataclasses import dataclass

import polars as pl
from psycopg import sql

from pipeline.mapping_config import TableMapping
from pipeline.target_schema import read_target_schema, polars_dtype


@dataclass(frozen=True)
class LookupUse:
    """One lookup the mappings need: find `ret` in `table`, matching on the columns in `keys`."""
    table : str                 # target table that is searched, for example machine_information
    keys : tuple[str, ...]      # columns of that table that are matched, for example ('interface_id',)
    ret : str                   # column that is returned, for example 'iot_id'


class LookupCache:
    """Loads lookup frames from Postgres on first use and keeps them for the rest of the run.

    A frame is loaded the first time a table needs it. The load order guarantees that the
    looked-up table has already been loaded by then (a child table loads after its parents).
    """

    def __init__(self, pg_conn , tables : dict[str, TableMapping]):
        self.pg_conn = pg_conn
        self.tables = tables
        self._frames : dict[LookupUse, pl.DataFrame] = {}
        self.duplicate_keys : dict[LookupUse, int] = {}      # how many duplicate keys were found, for the report

    def get(self, use : LookupUse) -> pl.DataFrame:
        """Frame with the columns (*keys, ret): one row per key, no NULL keys."""
        if use not in self._frames:
            self._frames[use] = self._load(use)
        return self._frames[use]

    def forget(self, table : str) -> None:
        """Drop cached frames of a table (call after that table has been (re)loaded)."""
        for use in [u for u in self._frames if u.table == table]:
            del self._frames[use]

    def _load(self, use : LookupUse) -> pl.DataFrame:
        m = self.tables[use.table]
        schema_name = (m.raw.get("target") or {}).get("schema", "public")
        target_cols = read_target_schema(self.pg_conn, schema_name, m.tgt_table)

        wanted = [*use.keys, use.ret]
        dtypes = {c: polars_dtype(target_cols[c].pg_type) for c in wanted}

        query = sql.SQL("SELECT {cols} FROM {tbl}").format(
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in wanted),
            tbl=sql.Identifier(schema_name, m.tgt_table),
        )
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        # An explicit schema keeps the types right even when the table is empty.
        df = pl.DataFrame(rows, schema=dtypes, orient="row")

        # A NULL key can never match, so drop those rows.
        df = df.drop_nulls(subset=list(use.keys))

        # Keys must be unique, otherwise a join would multiply the source rows.
        # Keep the lowest returned value so the choice is the same on every run.
        unique = df.sort(use.ret).unique(subset=list(use.keys), keep="first", maintain_order=True)
        duplicates = df.height - unique.height
        if duplicates:
            self.duplicate_keys[use] = duplicates

        return unique
