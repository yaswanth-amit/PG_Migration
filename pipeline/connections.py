import pyodbc
import psycopg
import json
import os
from contextlib import contextmanager
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent      # the project folder
CONFIG_PATH = BASE_DIR/"config"/"connections.yaml"
ENV_PATH =  BASE_DIR / ".env"
CONNECT_TIMEOUT_SECONDS = 30


def _load_dotenv(env_file : Path = ENV_PATH) -> None:
    if not env_file.exists():
        return

    for line in env_file.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or "=" not in line:
            continue

        key,value = line.split("=",1)
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key,value)


def _get_env_var(var_name : str , context : str) -> str:
    val = os.getenv(var_name)
    if not val:
        raise ValueError(
            f"Missing required environment variable '{var_name}' for {context}. "
            f"Please define it in your .env or system environment."
        )
    
    return val


def _require(value, name : str, section : str):
    if value is None or value == "":
        raise ValueError(f"Missing '{name}' in the '{section}' section of connections.yaml")
    return value


# Settings typed on the screen (ui/conn_form.py). Two ways in, both checked before connections.yaml:
#  * use_runtime_settings(): in this process (the Streamlit page), memory only.
#  * MIGRATION_CONNECTIONS_JSON in the environment: how the page hands them to the migration process it
#    starts. An environment variable, not a command-line argument, so the passwords never show in a
#    process list or a log, and are never written to disk.
ENV_SETTINGS = "MIGRATION_CONNECTIONS_JSON"
_RUNTIME : dict | None = None


def use_runtime_settings(settings : dict | None) -> None:
    """Use these settings instead of connections.yaml (None: back to the file)."""
    global _RUNTIME
    _RUNTIME = settings


def runtime_settings() -> dict | None:
    """The typed settings in force: set in this process, else handed over by the environment, else None."""
    if _RUNTIME:
        return _RUNTIME
    text = os.environ.get(ENV_SETTINGS)
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except ValueError as e:
        raise ValueError(f"{ENV_SETTINGS} is not valid JSON") from e
    if not isinstance(parsed, dict) or not {"source", "target"} <= set(parsed):
        raise ValueError(f"{ENV_SETTINGS} must hold 'source' and 'target' settings")
    return parsed


def load_settings(config_path : Path = CONFIG_PATH) -> dict:
    _load_dotenv()

    typed = runtime_settings()
    if typed:
        return {"source": {"driver": "ODBC Driver 17 for SQL Server", "trust_server_certificate": "yes", "user": None,
                           "password": None, **typed["source"]},
                "target": {"port": 5432, "user": None, "password": None, **typed["target"]}}

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path,'r',encoding='utf-8') as f:
        raw_cfg = yaml.safe_load(f) or {}

    # Resolve SQL SERVER
    src = raw_cfg.get('source',{})
    src_user_var = src.get('user_env')
    src_pass_var = src.get('password_env')

    source_settings = {
        "driver":src.get("driver", "ODBC Driver 17 for SQL Server"),
        "server": _require(src.get("server"), "server", "source"),
        "database": _require(src.get("database"), "database", "source"),
        "trust_server_certificate": src.get("trust_server_certificate", "yes"),
        "user":_get_env_var(src_user_var,"source database user") if src_user_var else None,
        "password":_get_env_var(src_pass_var,"source database password") if src_pass_var else None,
    }

    # Resolve POSTGRES
    tgt = raw_cfg.get('target',{})
    tgt_user_var = tgt.get('user_env')
    tgt_pass_var = tgt.get('password_env')

    target_settings = {
        "host": _require(tgt.get("host"), "host", "target"),
        "port": tgt.get("port", 5432),
        "database": _require(tgt.get("database"), "database", "target"),
        "user": _get_env_var(tgt_user_var, "target database user") if tgt_user_var else None,
        "password": _get_env_var(tgt_pass_var, "target database password") if tgt_pass_var else None,
    }

    return {
        "source":source_settings,
        "target":target_settings
    }


@contextmanager
def source_connection():
    settings = load_settings()['source']

    conn_str = (
        f"DRIVER={{{settings['driver']}}};"
        f"SERVER={settings['server']};"
        f"DATABASE={settings['database']};"
        f"TrustServerCertificate={settings['trust_server_certificate']};"
    )

    if settings["user"]:
        # Braces protect ';' and other special characters; a literal '}' must be doubled.
        pwd = (settings["password"] or "").replace("}", "}}")
        conn_str += f"UID={settings['user']};PWD={{{pwd}}};"
    else:
        conn_str += "Trusted_Connection=yes;"

    conn = pyodbc.connect(conn_str, timeout=CONNECT_TIMEOUT_SECONDS)
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def target_connection():
    settings = load_settings()["target"]

    conn = psycopg.connect(
        host=settings["host"],
        port=settings["port"],
        dbname=settings["database"],
        user=settings["user"],
        password=settings["password"],
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()



if __name__=="__main__":
    print("---Testing SQL Server connection ---")

    try:
        with source_connection() as src_conn:
            cursor = src_conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print('Connected successfully!')
            print(f"Version: {version.splitlines()[0]}\n")

    except Exception as e:
        print(f"Source connection failed: {e}\n")

    print("--- Testing Target (PostgreSQL) ---")

    try:
        with target_connection() as tgt_conn:
            with tgt_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print("Connected successfully!")
                print(f"Version: {version}\n")
    except Exception as e:
        print(f"Target connection failed: {e}\n")
    