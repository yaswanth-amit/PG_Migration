"""Field mode, developer mode and the typed connection settings. No database, no browser, no real run:
the page runs headless in offline mode and the process launch is replaced.
Run with:  python -m pytest tests/test_field_mode.py -q
"""
import hashlib
import json
import os
from pathlib import Path

os.environ["PG_MIGRATION_UI_OFFLINE"] = "1"                 # before anything reads it

import pytest
from streamlit.testing.v1 import AppTest

from access import dev_access
from pipeline import connections
from ui import conn_form as cf
from ui import services

APP = str(Path(__file__).resolve().parents[1] / "ui_app.py")
TYPED = {"cf_src_server": "SITE-PC\\SQLEXPRESS", "cf_src_database": "TPM", "cf_src_user": "reader",
         "cf_src_password": "s3cr}et", "cf_tgt_host": "10.0.0.5", "cf_tgt_port": "5433",
         "cf_tgt_database": "tpm_target", "cf_tgt_user": "loader", "cf_tgt_password": "pg-secret"}
REAL_SAVE_LAST = cf.save_last                              # the autouse fixture replaces it for the page tests
GOOD = {"source": (True, "SITE-PC / TPM"), "target": (True, "10.0.0.5:5433 / tpm_target"), "tracking": True, "ok": True}


class FakeProcess:
    def poll(self):
        return None

    def terminate(self):
        pass


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    """No real config, .env or remembered file; the dev password is set only here."""
    monkeypatch.setattr(connections, "_load_dotenv", lambda *a, **k: None)
    monkeypatch.delenv("MIGRATION_DEV_PASSWORD", raising=False)
    monkeypatch.delenv(connections.ENV_SETTINGS, raising=False)
    monkeypatch.setenv("MIGRATION_DEV_PASSWORD_SHA256", hashlib.sha256(b"dev-pw").hexdigest())
    monkeypatch.setattr(cf, "default_values", lambda *a, **k: {
        "src_server": "", "src_database": "", "src_windows": False, "src_user": "", "src_password": "",
        "src_driver": "", "src_trust": True, "tgt_host": "", "tgt_port": "5432", "tgt_database": "",
        "tgt_user": "", "tgt_password": ""})
    monkeypatch.setattr(cf, "save_last", lambda values, *a, **k: None)
    monkeypatch.setattr(services, "latest_progress_path", lambda: None)       # never read the real logs folder
    yield
    connections.use_runtime_settings(None)


def field_app(dev=False):
    at = AppTest.from_file(APP, default_timeout=60)
    at.session_state["dev"] = dev
    return at.run()


def button(at, label):
    return next(b for b in at.button if b.label == label)


def fill(at, **overrides):
    for key, value in {**TYPED, **overrides}.items():
        at.text_input(key=key).set_value(value)
    at.run()


def connect(at, monkeypatch, result=GOOD):
    fill(at)
    monkeypatch.setattr(cf, "test_connection", lambda settings, values=None: result)
    button(at, "Test connection").click().run()


# ---------------------------------------------------------------- the field screen
def test_field_mode_shows_only_database_migrate_and_live_progress():
    at = field_app()
    assert not at.exception
    assert [t.label for t in at.tabs] == ["🔌  Database", "🚀  Migrate", "📡  Live progress"]
    assert not [c for c in at.checkbox if c.key and c.key.startswith("sel_")]        # no table choice
    assert not [s for s in at.selectbox if s.key == "mapping_choice"]               # no mapping
    assert at.sidebar.caption[0].value == "Field mode"
    assert not any(b.label in ("Select all", "Masters only", "Add required tables") for b in at.button)


def test_field_mode_has_no_rehearsal_only_the_real_migration():
    at = field_app()
    assert not [r for r in at.radio if r.key == "mode"]                          # no mode choice
    assert any(b.label == "Start migration" for b in at.button)
    assert not any("ehearsal" in b.label for b in at.button)
    assert not any("ehearsal" in c.value for c in at.sidebar.caption)
    assert any("changes the database" in w.value for w in at.warning)            # the real-run warning is always there


def test_field_mode_cannot_start_before_the_connection_is_tested():
    at = field_app()
    assert button(at, "Start migration").disabled
    assert any("Test the database connection first" in c.value for c in at.caption)


def test_an_empty_form_is_refused_with_plain_messages():
    at = field_app()
    button(at, "Test connection").click().run()
    text = " ".join(e.value for e in at.error)
    assert "SQL Server: server is empty" in text and "PostgreSQL: password is empty" in text
    assert at.session_state["conn_settings"] is None


def test_a_good_test_builds_the_settings_from_what_was_typed(monkeypatch):
    at = field_app()
    connect(at, monkeypatch)
    s = at.session_state["conn_settings"]
    assert s["source"]["server"] == "SITE-PC\\SQLEXPRESS" and s["source"]["password"] == "s3cr}et"
    assert s["target"]["port"] == 5433 and s["target"]["database"] == "tpm_target"
    assert any("Connected" in i.value for i in at.info)
    assert any("tpm_target" in m.value for m in at.sidebar.markdown)            # the side card names the target
    assert button(at, "Start migration").disabled                                # until the database name is typed
    assert any("Type the exact target database name" in c.value for c in at.caption)


def test_a_failed_test_shows_which_side_failed_and_never_the_password(monkeypatch):
    at = field_app()
    bad = {"source": (False, "Login failed. Check the user name and password."),
           "target": (True, "10.0.0.5:5433 / tpm_target"), "tracking": True, "ok": False}
    connect(at, monkeypatch, bad)
    text = " ".join(e.value for e in at.error)
    assert "SQL Server failed" in text and "Check the user name and password" in text
    assert "s3cr}et" not in text and at.session_state["conn_settings"] is None
    assert button(at, "Start migration").disabled


def test_changing_the_settings_after_the_test_blocks_the_start(monkeypatch):
    at = field_app()
    connect(at, monkeypatch)
    at.text_input(key="cf_tgt_host").set_value("other-host").run()
    assert button(at, "Start migration").disabled
    assert any("changed after the test" in c.value for c in at.caption)


def test_a_missing_tracking_schema_is_explained_and_blocks_the_run(monkeypatch):
    at = field_app()
    connect(at, monkeypatch, dict(GOOD, tracking=False))
    assert any("no 'migration' schema" in w.value for w in at.warning)
    at.text_input(key="confirm_target").set_value("tpm_target").run()
    assert button(at, "Start migration").disabled                      # offline: no tracking schema
    assert any("needs the migration tracking schema" in c.value for c in at.caption)


def test_the_target_database_name_must_be_typed_exactly(monkeypatch):
    monkeypatch.setattr(services, "fetch_status", lambda: ({}, None))   # pretend the tracking schema exists
    at = field_app()
    connect(at, monkeypatch)
    assert any("changes the database **tpm_target**" in w.value for w in at.warning)
    at.text_input(key="confirm_target").set_value("wrong_name").run()
    assert button(at, "Start migration").disabled
    at.text_input(key="confirm_target").set_value("tpm_target").run()
    assert not button(at, "Start migration").disabled


def test_start_launches_a_real_run_of_all_tables_and_hands_the_passwords_over_outside_the_command(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "fetch_status", lambda: ({}, None))
    at = field_app()
    connect(at, monkeypatch)
    at.text_input(key="confirm_target").set_value("tpm_target").run()
    seen = {}

    def fake_launch(command, settings=None):
        seen.update(command=command, settings=settings)
        return FakeProcess(), tmp_path / "launch.txt"

    monkeypatch.setattr(services, "launch_run", fake_launch)
    button(at, "Start migration").click().run()
    assert not at.exception
    cmd = seen["command"]
    assert cmd[cmd.index("--tables") + 1] == "all" and cmd[cmd.index("--mode") + 1] == "real"
    assert cmd[cmd.index("--confirm-target") + 1] == "tpm_target"
    assert cmd[cmd.index("--max-windows") + 1] == "0"                    # every window
    assert "--days" not in cmd and "--no-resume" not in cmd              # the other window options stay at their defaults
    assert "s3cr}et" not in " ".join(cmd) and "pg-secret" not in " ".join(cmd)
    assert seen["settings"]["target"]["password"] == "pg-secret"
    assert at.session_state["run_options"]["mode"] == "real"


def test_developer_mode_still_has_the_rehearsal_choice():
    at = field_app(dev=True)
    assert at.radio(key="mode").options == ["Rehearsal (keeps nothing)", "Real migration"]
    assert any(b.label == "Start rehearsal" for b in at.button)


def test_the_run_environment_carries_the_settings_only_when_given():
    settings = {"source": {"server": "s", "database": "d"}, "target": {"host": "h", "database": "t"}}
    env = services.run_environment(settings)
    assert json.loads(env[connections.ENV_SETTINGS]) == settings
    os.environ[connections.ENV_SETTINGS] = "stale"
    try:
        assert connections.ENV_SETTINGS not in services.run_environment(None)
    finally:
        del os.environ[connections.ENV_SETTINGS]


# ---------------------------------------------------------------- developer mode
def test_developer_login_wrong_then_right_password():
    at = field_app()
    at.sidebar.text_input[0].set_value("nope")
    next(b for b in at.sidebar.button if b.label == "Unlock").click()
    at.run()
    assert any("Wrong password" in e.value for e in at.sidebar.error)
    assert at.session_state["dev"] is False
    at.sidebar.text_input[0].set_value("dev-pw")
    next(b for b in at.sidebar.button if b.label == "Unlock").click()
    at.run()
    assert at.session_state["dev"] is True and not at.exception
    assert len(at.tabs) == 5


def test_developer_login_is_unavailable_without_a_configured_password(monkeypatch):
    monkeypatch.delenv("MIGRATION_DEV_PASSWORD_SHA256")
    at = field_app()
    assert any("not set up" in c.value for c in at.sidebar.caption)
    assert not [b for b in at.sidebar.button if b.label == "Unlock"]


def test_developer_mode_has_the_table_choice_mapping_and_history_tabs():
    at = field_app(dev=True)
    assert [t.label for t in at.tabs] == ["🔌  Database", "🚀  Migrate", "📡  Live progress",
                                          "🗺️  Mapping", "🕘  History"]
    assert len([c for c in at.checkbox if c.key and c.key.startswith("sel_")]) == 30
    assert any(n.key == "days" for n in at.number_input) is False          # autodata not selected yet
    at.checkbox(key="sel_autodata").check().run()
    assert any(n.key == "days" for n in at.number_input) and any(n.key == "max_windows" for n in at.number_input)
    assert any(e.label == "Advanced (developer)" for e in at.expander)


def test_field_mode_autodata_has_only_the_date_range_and_no_window_options():
    at = field_app()
    assert any(c.key == "limit_range" for c in at.checkbox)
    assert not any(n.key in ("days", "max_windows") for n in at.number_input)
    assert not any(c.key == "resume" for c in at.checkbox)
    assert not any(e.label == "Advanced (developer)" for e in at.expander)


# ---------------------------------------------------------------- the connection helpers
VALUES = {"src_server": "S", "src_database": "D", "src_windows": False, "src_user": "u", "src_password": "p}w",
          "src_driver": "ODBC Driver 17 for SQL Server", "src_trust": True,
          "tgt_host": "h", "tgt_port": "5432", "tgt_database": "t", "tgt_user": "tu", "tgt_password": "tp"}


def test_validate_and_build():
    assert cf.validate(VALUES) == []
    assert "PostgreSQL: port must be a number between 1 and 65535." in cf.validate(dict(VALUES, tgt_port="70000"))
    assert "SQL Server: user is empty." in cf.validate(dict(VALUES, src_user=" "))
    s = cf.build_settings(VALUES)
    assert s["source"]["password"] == "p}w" and s["target"]["port"] == 5432
    assert s["source"]["trust_server_certificate"] == "yes"


def test_a_windows_login_needs_no_user_or_password_and_builds_a_trusted_connection():
    v = dict(VALUES, src_windows=True, src_user="", src_password="")
    assert cf.validate(v) == []
    settings = cf.build_settings(v)
    assert settings["source"]["user"] is None and settings["source"]["password"] is None
    connections.use_runtime_settings(settings)
    assert connections.load_settings()["source"]["user"] is None


def test_runtime_settings_win_over_the_yaml_and_the_environment_hands_them_to_the_run(monkeypatch):
    typed = cf.build_settings(VALUES)
    connections.use_runtime_settings(typed)
    loaded = connections.load_settings()
    assert loaded["source"]["server"] == "S" and loaded["target"]["database"] == "t"
    connections.use_runtime_settings(None)
    monkeypatch.setenv(connections.ENV_SETTINGS, json.dumps(typed))
    assert connections.load_settings()["target"]["password"] == "tp"            # what the started process sees
    monkeypatch.setenv(connections.ENV_SETTINGS, "{not json")
    with pytest.raises(ValueError):
        connections.runtime_settings()
    monkeypatch.setenv(connections.ENV_SETTINGS, json.dumps({"source": {}}))
    with pytest.raises(ValueError):
        connections.runtime_settings()


def test_connection_info_describes_typed_settings_without_secrets():
    connections.use_runtime_settings(cf.build_settings(VALUES))
    info = services.connection_info()
    assert info["target_database"] == "t" and "S / D" == info["source"]
    assert "tp" not in str(info) and "p}w" not in str(info)


def test_only_non_secret_fields_are_remembered(tmp_path):
    path = tmp_path / "last.json"
    REAL_SAVE_LAST(VALUES, path)
    text = path.read_text()
    assert "p}w" not in text and "tgt_password" not in text and "src_password" not in text
    assert "tp" not in json.loads(text).values()
    assert cf.load_last(path)["tgt_host"] == "h"
    path.write_text(json.dumps({"src_password": "x", "evil": 1, "tgt_host": "h2"}))
    assert cf.load_last(path) == {"tgt_host": "h2"}                  # a hand-edited file cannot inject a password


def test_scrub_and_friendly_error_never_show_a_password():
    msg = cf.friendly_error(RuntimeError("Login failed for user 'u' with p}w"), VALUES)
    assert "p}w" not in msg and "Check the user name and password" in msg
    assert "ODBC" in cf.friendly_error(RuntimeError("[IM002] Data source name not found"), VALUES)


def test_the_test_restores_the_settings_that_were_in_force(monkeypatch):
    before = {"source": {"server": "before"}, "target": {"host": "before"}}
    connections.use_runtime_settings(before)
    bad = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom p}w"))     # noqa: E731
    monkeypatch.setattr(connections, "source_connection", bad)
    monkeypatch.setattr(connections, "target_connection", bad)
    result = cf.test_connection(cf.build_settings(VALUES), VALUES)
    assert result["ok"] is False and "p}w" not in result["source"][1] + result["target"][1]
    assert connections._RUNTIME is before


def test_dev_password_hash_plain_and_none(monkeypatch):
    assert dev_access.is_configured() and dev_access.check_password("dev-pw")
    assert not dev_access.check_password("dev-pw ") and not dev_access.check_password("")
    monkeypatch.delenv("MIGRATION_DEV_PASSWORD_SHA256")
    assert not dev_access.is_configured() and not dev_access.check_password("")
    monkeypatch.setenv("MIGRATION_DEV_PASSWORD", "plain")
    assert dev_access.check_password("plain") and not dev_access.check_password("other")


# ---------------------------------------------------------------- the result check on the page
def finished_progress(tmp_path, monkeypatch, tables, status):
    snap = {"run_id": "abcdef12-0000-0000-0000-000000000000", "mode": "real", "status": status, "phase": "done",
            "pid": None, "started_ts": 1_000_000_000.0, "updated_ts": 1_000_000_060.0, "finished_ts": 1_000_000_060.0,
            "tables": tables}
    path = tmp_path / "progress.json"
    path.write_text(json.dumps(snap), encoding="utf-8")
    monkeypatch.setattr(services, "latest_progress_path", lambda: path)
    return snap


def t(name, status="completed", total=10, read=10, loaded=10, held=0):
    return {"name": name, "status": status, "rows_total": total, "rows_read": read, "rows_loaded": loaded,
            "rows_excepted": held, "stage": None, "windows_done": 0, "windows_total": 0, "error": None,
            "started_ts": None, "finished_ts": None}


def test_a_finished_clean_run_shows_the_100_percent_verdict(monkeypatch, tmp_path):
    finished_progress(tmp_path, monkeypatch, [t("a"), t("b")], "completed")
    at = field_app()
    assert any("Result check" in m.value for m in at.markdown)
    assert any("Rehearsal passed" in s.value or "100% migrated" in s.value for s in at.success)
    assert not any("What to do now" in m.value for m in at.markdown)             # nothing to send


def test_an_incomplete_run_shows_the_tables_and_offers_the_support_package(monkeypatch, tmp_path):
    finished_progress(tmp_path, monkeypatch, [t("a"), t("b", "completed_with_exceptions", 10, 10, 7, 3)],
                      "completed_with_exceptions")
    at = field_app()
    assert any("Not 100%" in w.value for w in at.warning)
    frames = [d.value for d in at.dataframe]
    assert any("Problem" in f.columns and "3 row(s) held back" in f["Problem"].tolist() for f in frames)
    assert any("What to do now" in m.value and "support package" in m.value for m in at.markdown)
    assert not at.get("download_button")                                         # built only when asked
    button(at, "Download the support package (.zip)").click().run()
    assert not at.exception, at.exception
    labels = [d.label for d in at.get("download_button")]
    assert len(labels) == 1 and labels[0].startswith("⬇ Save migration_support_abcdef12_")


def test_a_failed_run_is_shown_as_an_error_with_the_package(monkeypatch, tmp_path):
    finished_progress(tmp_path, monkeypatch, [t("a"), t("b", "failed", 10, 3, 0)], "failed")
    at = field_app()
    assert any("The run failed at b" in e.value for e in at.error)
    assert any(b.label == "Download the support package (.zip)" for b in at.button)
