"""Live progress of a run, written to a small JSON file that the web page reads while the run is going.

Why a file and not the migration.* tables: the tracking tables only get row counts when a table (or an
autodata window) finishes, and their schema is under DBA review. A progress file gives a smooth live counter
without touching that schema, and it also works for rehearsals without tracking.

Only names, counts and times are written, never row values. The reporter must never break a migration:
every write error is swallowed.
"""
import json
import os
import time
from pathlib import Path

DONE = ("completed", "completed_with_exceptions")
FINISHED = DONE + ("failed", "blocked", "cancelled")


class ProgressReporter:
    def __init__(self, path : Path | str , min_interval : float = 0.4):
        self.path = Path(path)
        self.min_interval = min_interval
        self._last_write = 0.0
        self.state : dict = {"run_id": None, "mode": None, "status": "starting", "phase": "preparing", "pid": os.getpid(), "started_ts": time.time(),
                             "updated_ts": time.time(), "finished_ts": None, "tables": []}
        self._index : dict[str, dict] = {}

    # ---- run
    def start_run(self , run_id : str , mode : str , names : list[str]) -> None:
        self.state.update(run_id=run_id, mode=mode, status="running", started_ts=time.time())
        self.state["tables"] = [self._blank(n) for n in names]
        self._index = {t["name"]: t for t in self.state["tables"]}
        self._flush(force=True)

    def set_totals(self , totals : dict[str, int | None]) -> None:
        for name, total in totals.items():
            if name in self._index:
                self._index[name]["rows_total"] = total
        self._flush(force=True)

    def phase(self , phase : str) -> None:
        """Run-level stage: preparing, counting, migrating, finalizing."""
        self.state["phase"] = phase
        self._flush(force=True)

    def finish_run(self , status : str) -> None:
        self.state.update(status=status, finished_ts=time.time(), phase="done")
        for t in self.state["tables"]:
            t["stage"] = None
        self._flush(force=True)

    def table_stage(self , name : str , stage : str | None) -> None:
        """Table-level stage: extract, transform, load, record. Throttled like progress updates."""
        t = self._index.get(name)
        if t and t.get("stage") != stage:
            t["stage"] = stage
            self._flush()

    # ---- tables
    def table_started(self , name : str) -> None:
        t = self._index.get(name)
        if t:
            t.update(status="running", started_ts=time.time())
            self._flush(force=True)

    def table_windows(self , name : str , windows_total : int , windows_done : int = 0) -> None:
        t = self._index.get(name)
        if t:
            t.update(windows_total=windows_total, windows_done=windows_done)
            self._flush(force=True)

    def table_progress(self , name : str , rows_read : int , rows_excepted : int = 0 ,
                       windows_done : int | None = None , force : bool = False) -> None:
        """Called after every batch. Rows loaded so far is estimated as read minus held back; the final
        numbers replace the estimate in table_finished."""
        t = self._index.get(name)
        if not t:
            return
        t.update(rows_read=rows_read, rows_excepted=rows_excepted, rows_loaded=max(rows_read - rows_excepted, 0))
        if windows_done is not None:
            t["windows_done"] = windows_done
        self._flush(force=force)

    def table_finished(self , name : str , status : str , rows_read : int = 0 , rows_loaded : int = 0 ,
                       rows_excepted : int = 0 , windows_done : int | None = None , error : str | None = None) -> None:
        t = self._index.get(name)
        if not t:
            return
        t.update(status=status, rows_read=rows_read, rows_loaded=rows_loaded, rows_excepted=rows_excepted,
                 finished_ts=time.time(), error=error, stage=None)
        if windows_done is not None:
            t["windows_done"] = windows_done
        if status in DONE and t.get("rows_total") is not None:
            t["rows_total"] = max(t["rows_total"], rows_read)       # a count taken earlier may be slightly off
        self._flush(force=True)

    # ---- plumbing
    @staticmethod
    def _blank(name : str) -> dict:
        return {"name": name, "status": "pending", "rows_total": None, "rows_read": 0, "rows_loaded": 0,
                "rows_excepted": 0, "stage": None, "windows_done": 0, "windows_total": 0, "started_ts": None,
                "finished_ts": None, "error": None}

    def _flush(self , force : bool = False) -> None:
        now = time.time()
        if not force and now - self._last_write < self.min_interval:
            return
        self._last_write = now
        self.state["updated_ts"] = now
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state), encoding="utf-8")
            for _ in range(5):                           # a reader may hold the file for a moment on Windows
                try:
                    os.replace(tmp, self.path)
                    break
                except PermissionError:
                    time.sleep(0.05)
        except Exception:                                # noqa: BLE001 - progress must never break a migration
            pass


class NullProgress:
    """Does nothing. Used when no progress file was asked for."""

    def __getattr__(self , name):
        def ignore(*args , **kwargs):
            return None
        return ignore


NULL_PROGRESS = NullProgress()
