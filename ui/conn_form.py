"""The Database tab, without Streamlit: check what was typed, build the settings, test both databases.

Rules:
* Passwords are held in memory for the browser session only. They are never written to disk and never
  put into a message shown on screen (`scrub`).
* Only the non-secret fields (server, database, user, port ...) are remembered between runs, in
  config/last_connection.json, so the next start is pre-filled.
* The settings reach the migration through pipeline.connections (use_runtime_settings in this process,
  MIGRATION_CONNECTIONS_JSON in the environment of the process the page starts).
"""
import json
from pathlib import Path

import yaml

from pipeline import connections

BASE_DIR = Path(__file__).resolve().parent.parent
LAST_FILE = BASE_DIR / "config" / "last_connection.json"

# Form field -> remembered between runs? (passwords never are)
FIELDS = {
    "src_server": True, "src_database": True, "src_windows": True, "src_user": True, "src_password": False,
    "src_driver": True, "src_trust": True,
    "tgt_host": True, "tgt_port": True, "tgt_database": True, "tgt_user": True, "tgt_password": False,
}
DEFAULT_PORT = "5432"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"


# ----------------------------------------------------------------------------- form values
def sql_drivers() -> list[str]:
    """SQL Server ODBC drivers installed on this PC, newest first. Empty if none or pyodbc is unusable."""
    try:
        import pyodbc
        return sorted((d for d in pyodbc.drivers() if "SQL Server" in d), reverse=True)
    except Exception:                                   # noqa: BLE001 - the form falls back to a typed name
        return []


def _from_yaml(path : Path = connections.CONFIG_PATH) -> dict:
    """Names from config/connections.yaml when it exists (developer PCs). Never fails, never reads secrets."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    src, tgt = raw.get("source") or {}, raw.get("target") or {}
    return {k: v for k, v in {
        "src_server": str(src.get("server", "")).strip(), "src_database": str(src.get("database", "")).strip(),
        "src_driver": str(src.get("driver", "")).strip(),
        "tgt_host": str(tgt.get("host", "")).strip(), "tgt_port": str(tgt.get("port", "")).strip(),
        "tgt_database": str(tgt.get("database", "")).strip(),
    }.items() if v}


def load_last(path : Path = LAST_FILE) -> dict:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {k: v for k, v in raw.items() if FIELDS.get(k)} if isinstance(raw, dict) else {}


def save_last(values : dict, path : Path = LAST_FILE) -> None:
    """Remember the non-secret fields. Best effort: a read-only folder must not break the page."""
    try:
        path.write_text(json.dumps({k: v for k, v in values.items() if FIELDS.get(k)}, indent=2), encoding="utf-8")
    except OSError:
        pass


def default_values(yaml_path : Path = connections.CONFIG_PATH, last_path : Path = LAST_FILE) -> dict:
    """Blank form, then connections.yaml, then what was used last time."""
    base = {"src_server": "", "src_database": "", "src_windows": False, "src_user": "", "src_password": "",
            "src_driver": "", "src_trust": True,
            "tgt_host": "", "tgt_port": DEFAULT_PORT, "tgt_database": "", "tgt_user": "", "tgt_password": ""}
    base.update(_from_yaml(yaml_path))
    base.update(load_last(last_path))
    return base


# ----------------------------------------------------------------------------- check and build
def validate(values : dict) -> list[str]:
    """What is wrong with the typed values, in plain words. Empty list = good to try."""
    problems = []
    required = [("src_server", "SQL Server: server"), ("src_database", "SQL Server: database"),
                ("tgt_host", "PostgreSQL: host"), ("tgt_database", "PostgreSQL: database"),
                ("tgt_user", "PostgreSQL: user")]
    if not values.get("src_windows"):
        required.append(("src_user", "SQL Server: user"))
    problems += [f"{label} is empty." for key, label in required if not str(values.get(key, "")).strip()]
    if not values.get("src_windows") and not values.get("src_password"):
        problems.append("SQL Server: password is empty.")
    if not values.get("tgt_password"):
        problems.append("PostgreSQL: password is empty.")
    port = str(values.get("tgt_port", "")).strip()
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        problems.append("PostgreSQL: port must be a number between 1 and 65535.")
    return problems


def build_settings(values : dict) -> dict:
    """The structure pipeline.connections expects. A Windows login means no user and no password."""
    windows = bool(values.get("src_windows"))
    driver = (values.get("src_driver") or "").strip() or next(iter(sql_drivers()), DEFAULT_DRIVER)
    return {
        "source": {"driver": driver, "server": values["src_server"].strip(), "database": values["src_database"].strip(),
                   "trust_server_certificate": "yes" if values.get("src_trust", True) else "no",
                   "user": None if windows else values["src_user"].strip(),
                   "password": None if windows else values["src_password"]},
        "target": {"host": values["tgt_host"].strip(), "port": int(str(values["tgt_port"]).strip()),
                   "database": values["tgt_database"].strip(), "user": values["tgt_user"].strip(),
                   "password": values["tgt_password"]},
    }


def signature(values : dict) -> str:
    """Identifies the typed values, to notice that they changed after a successful test."""
    return repr(sorted((k, v) for k, v in values.items() if k in FIELDS))


def scrub(text : str, values : dict) -> str:
    """Remove any typed password from a message before it is shown."""
    for key in ("src_password", "tgt_password"):
        secret = values.get(key)
        if secret:
            text = text.replace(secret, "***")
    return text


def friendly_error(exc : Exception, values : dict) -> str:
    """One readable message: the error without any password, plus a hint when the cause is common."""
    text = scrub(f"{type(exc).__name__}: {exc}", values)
    low = text.lower()
    hint = ""
    if "login failed" in low or "password authentication failed" in low:
        hint = "Check the user name and password."
    elif any(w in low for w in ("timeout", "could not connect", "network-related", "connection refused", "08001",
                                "could not translate host", "name or service not known")):
        hint = "Check the server name / host and the port, and that this PC can reach the server."
    elif "data source name not found" in low or "im002" in low:
        hint = "The SQL Server ODBC driver was not found. Ask IT to install ODBC Driver 17 or 18."
    elif "does not exist" in low or "cannot open database" in low:
        hint = "Check the database name."
    return f"{text} {hint}".strip()


# ----------------------------------------------------------------------------- test
def test_connection(settings : dict, values : dict | None = None) -> dict:
    """Open both databases once, read-only, and report each side on its own.
    Returns {"source": (ok, text), "target": (ok, text), "tracking": True/False/None, "ok": bool}.
    The settings in force before the test are put back afterwards."""
    values = values or {}
    previous = connections._RUNTIME
    connections.use_runtime_settings(settings)
    result = {"source": (False, ""), "target": (False, ""), "tracking": None}
    try:
        try:
            with connections.source_connection() as conn:
                row = conn.cursor().execute("SELECT @@SERVERNAME, DB_NAME()").fetchone()
            result["source"] = (True, f"{row[0]} / {row[1]}")
        except Exception as e:                          # noqa: BLE001 - every cause is shown, scrubbed
            result["source"] = (False, friendly_error(e, values))
        try:
            with connections.target_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_database(), to_regclass('migration.run') IS NOT NULL")
                    name, tracking = cur.fetchone()
            result["target"] = (True, f"{settings['target']['host']}:{settings['target']['port']} / {name}")
            result["tracking"] = bool(tracking)
        except Exception as e:                          # noqa: BLE001
            result["target"] = (False, friendly_error(e, values))
    finally:
        connections.use_runtime_settings(previous)
    result["ok"] = result["source"][0] and result["target"][0]
    return result
