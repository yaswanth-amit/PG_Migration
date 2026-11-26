"""Streamlit page for the migration.   Start it with:   streamlit run ui_app.py

Two modes:
* Field mode (default, for the site team): 🔌 Database (type both connections and test them), 🚀 Migrate
  (rehearsal or real, nothing else to choose) and 📡 Live progress. All tables are migrated in their
  dependency order; no table choice, mapping or history.
* Developer mode (password, see pipeline/dev_access.py): the same tabs plus the table choice, the
  window options, 🗺️ Mapping and 🕘 History.

The page only READS the migration.* tables and the run's progress file, and STARTS main.py as a separate
process, so closing the browser never interrupts a run. Nothing here shows row values, only names, counts
and times. Passwords typed here stay in memory: they go to the run through its environment, never into a
file, a log or the command line.
"""
import json
import os
import time
from datetime import date, datetime

import pandas as pd
import streamlit as st

from access import dev_access
from pipeline import connections
from pipeline import mapping_overrides as mo
from pipeline.mapping_config import load_mappings
from pipeline.planner import plan_run, validate_selection, with_dependencies
from ui import components as view
from ui import completion, conn_form as cf, services

st.set_page_config(page_title="TPM Data Migration", page_icon="🚀", layout="wide")
st.markdown(view.CSS, unsafe_allow_html=True)


@st.cache_resource
def get_reviewed():
    return mo.load_reviewed()


@st.cache_resource
def get_mappings():
    return load_mappings()


@st.cache_data(ttl=300, show_spinner=False)
def source_range(signature):
    """`signature` only makes the cache follow the connection that is in use."""
    return services.fetch_source_range(get_mappings().tables["autodata"])


# ----------------------------------------------------------------------------- state: mode and connection
ss = st.session_state
ss.setdefault("dev", False)
ss.setdefault("conn_settings", None)       # settings of the last successful test (holds passwords: memory only)
ss.setdefault("conn_sig", None)            # which typed values that test used
ss.setdefault("conn_result", None)         # what the last test found, per database
for _key, _value in cf.default_values().items():
    ss.setdefault(f"cf_{_key}", _value)


def typed_values() -> dict:
    return {k: ss.get(f"cf_{k}") for k in cf.FIELDS}


dev = bool(ss.dev)
connections.use_runtime_settings(ss.conn_settings)      # None: connections.yaml and .env, as on a developer PC
connected = ss.conn_settings is not None
settings_changed = connected and ss.conn_sig != cf.signature(typed_values())

ms = get_mappings()
names = list(ms.load_order)
if dev or connected:
    status, status_error = services.fetch_status()
else:
    status, status_error = {}, "Enter the database details in the Database tab first."
completed = {n for n, row in status.items() if row.get("status") in services.DONE_STATUSES}
info = services.connection_info()


def selected_tables() -> set[str]:
    if not dev:
        return set(names)                  # field mode: everything, in dependency order
    return {n for n in names if st.session_state.get(f"sel_{n}")}


def select_all():
    for n in names:
        st.session_state[f"sel_{n}"] = True


def clear_all():
    for n in names:
        st.session_state[f"sel_{n}"] = False


def select_masters():
    for n in names:
        st.session_state[f"sel_{n}"] = "batching" not in ms.tables[n].raw


def add_required():
    for n in with_dependencies(selected_tables(), ms.tables, completed):
        st.session_state[f"sel_{n}"] = True


# ----------------------------------------------------------------------------- the run being watched
def current_run():
    """(snapshot or None, is_active, launch_error_text). The page may have started the run itself (then it
    holds the process) or may have been reloaded while a run goes on (then freshness of the file decides)."""
    process = st.session_state.get("proc")
    own_path = st.session_state.get("progress_path")
    path = own_path or services.latest_progress_path()
    snap = services.read_progress(path)
    alive = None
    if process is not None and path == own_path:
        alive = process.poll() is None
    if snap is None:
        if alive:                                                     # started, the first file is not there yet
            return {"status": "starting", "phase": "preparing", "mode": st.session_state.get("run_mode", "rehearsal"),
                    "started_ts": st.session_state.get("started_ts", 0), "tables": []}, True, None
        if process is not None and own_path and alive is False:       # it ended before it could report: show why
            return None, False, services.tail_text(st.session_state["launch_out"]) if st.session_state.get("launch_out") else None
        return None, False, None
    active = services.is_active(snap, alive)
    if snap.get("status") in ("running", "starting") and not active:
        snap = dict(snap, status="cancelled")                         # the process is gone without finishing
    return snap, active, None


def draw_run(snapshot, key, detail):
    """Draw the progress card with a smooth bar (it grows from where it was at the last refresh)."""
    summary = services.summarize_progress(snapshot)
    previous = st.session_state.get(f"prev_pct_{key}", 0.0)
    st.session_state[f"prev_pct_{key}"] = summary["pct"]
    st.markdown(view.live_card(snapshot, summary, previous, detail), unsafe_allow_html=True)
    return summary


snapshot_now, active_now, _ = current_run()


# ----------------------------------------------------------------------------- header and sidebar
st.markdown(view.hero(info), unsafe_allow_html=True)

with st.sidebar:
    if dev:
        st.success("Developer mode")
        if st.button("Back to field mode", key="leave_dev"):
            ss.dev = False
            st.rerun()
    else:
        st.caption("Field mode")
        with st.expander("Developer login"):
            if not dev_access.is_configured():
                st.caption("Developer mode is not set up on this PC.")
            else:
                with st.form("dev_login", clear_on_submit=True):
                    password = st.text_input("Password", type="password")
                    if st.form_submit_button("Unlock"):
                        if dev_access.check_password(password):
                            ss.dev = True
                            st.rerun()
                        time.sleep(1)                                  # slows down guessing
                        st.error("Wrong password.")
    st.markdown("### Run details")
    st.markdown(view.side_card("Source (SQL Server)", info.get("source", "unknown")), unsafe_allow_html=True)
    st.markdown(view.side_card("Target (PostgreSQL)", info.get("target", "unknown")), unsafe_allow_html=True)
    if status_error and (dev or connected):
        st.warning(status_error)
    st.markdown("### Pipeline")
    st.markdown(view.stage_track("", services.TABLE_STAGES, None, vertical=True), unsafe_allow_html=True)
    st.caption("Each table is **extracted** from SQL Server, **transformed** by its mapping, **loaded** into "
               "PostgreSQL and then **recorded** with its counts.")
    st.markdown("---")
    st.caption("A real migration keeps the data, so it needs testing and your normal change-control approval first."
               if not dev else
               "A rehearsal runs everything and rolls it back. A real migration keeps the data, so it needs "
               "testing and your normal change-control approval first.")


# ----------------------------------------------------------------------------- live banner (all tabs)
@st.fragment(run_every=1.0 if active_now else None)
def live_banner():
    snap, active, launch_error = current_run()
    if snap is not None:
        draw_run(snap, "banner", detail=True)
    elif launch_error:
        st.error("The run stopped before it started. Output:")
        st.code(launch_error, language="text")
    if st.session_state.get("was_active") and not active:
        st.session_state["was_active"] = False
        st.toast("The run has finished.", icon="🏁")
        st.rerun()
    st.session_state["was_active"] = active


live_banner()


# ----------------------------------------------------------------------------- result check (after a run)
def run_options_for(snapshot):
    """The choices made when this page started the run, if this snapshot is that run (else None)."""
    started = ss.get("started_ts")
    if ss.get("run_options") and started and abs((snapshot.get("started_ts") or 0) - started) < 600:
        return ss.run_options
    return None


def result_check(snapshot):
    """One clear verdict: 100% or not. If not, which tables, how many rows, why, and one file to send."""
    options = run_options_for(snapshot)
    report = completion.check(snapshot, options)
    verdict = report["verdict"]
    st.markdown("### Result check")
    (st.success if verdict in ("complete", "limited") else st.error if verdict in ("failed", "cancelled")
     else st.warning)(report["headline"])
    if not report["options_known"] and report["mode"] == "real" and verdict == "complete":
        st.caption("This page was reloaded after the start, so the date range and options of the run are not known. "
                   "Check them before you call this 100%.")
    if report["problems"]:
        st.dataframe(pd.DataFrame([{"Table": p["table"], "Source rows": p["source_rows"], "Read": p["rows_read"],
                                    "Loaded": p["rows_loaded"], "Held back": p["rows_held_back"],
                                    "Problem": p["problem"]} for p in report["problems"]]), hide_index=True)
        reasons = completion.reason_counts(completion.exception_files(snapshot))
        if not reasons and report["totals"]["rows_held_back"]:
            st.caption("The exception files of this run were not found (a later run replaced them), so the reasons "
                       "cannot be shown. Download the package straight after a run.")
        if reasons:
            st.markdown("**Why rows were held back**")
            st.dataframe(pd.DataFrame([{"Table": r["table"], "Reason": r["reason"], "Column": r["column"],
                                        "Rows": r["rows"], "What it means": r["meaning"]} for r in reasons]),
                         hide_index=True)
        contact = os.getenv("MIGRATION_SUPPORT_CONTACT")
        st.markdown("**What to do now**\n\n"
                    "1. Do not change anything in the databases and do not start another real migration.\n"
                    "2. Download the support package below.\n"
                    f"3. Send it to the migration team{f' ({contact})' if contact else ''}. "
                    "The team will decide the mapping or master changes and tell you when to run again.")
    key = (snapshot.get("run_id"), snapshot.get("finished_ts"))
    if verdict in ("none", "running"):
        return
    if ss.get("package_key") != key:
        ss.package_key, ss.package = key, None
    label = "Download the support package (.zip)" if report["problems"] else "Prepare the report (.zip)"
    if ss.get("package") is None and st.button(label, type="primary" if report["problems"] else "secondary",
                                                  key="make_package"):
        table_runs, _ = services.fetch_table_runs(snapshot["run_id"]) if (snapshot.get("run_id") and (connected or dev)) else ([], None)
        ss.package = completion.build_package(snapshot, options, info, table_runs)
        st.rerun()
    if ss.get("package") is not None:
        name, data = ss.package
        st.download_button(f"⬇ Save {name}", data, file_name=name, mime="application/zip", type="primary",
                           key="dl_package")
        st.caption("Table names, counts, reasons, record ids and the run log. No passwords. Internal use only.")


if snapshot_now is not None and not active_now and snapshot_now.get("tables"):
    result_check(snapshot_now)

labels =["🔌  Database", "🚀  Migrate", "📡  Live progress"] + (["🗺️  Mapping", "🕘  History"] if dev else [])
tabs = st.tabs(labels)
tab_db, tab_migrate, tab_live = tabs[:3]

# ----------------------------------------------------------------------------- database
with tab_db:
    st.markdown(view.step_heading(1, "Enter the two databases"), unsafe_allow_html=True)
    st.caption("SQL Server is the source, PostgreSQL is the target. Press **Test connection** when done. Passwords "
               "stay in this browser session and are never saved. Use the login your DBA gave you for this migration.")
    left, right = st.columns(2)
    with left:
        st.markdown("**SQL Server (source)**")
        st.text_input("Server (host\\instance)", key="cf_src_server")
        st.text_input("Database", key="cf_src_database")
        windows_login = st.checkbox("Use my Windows login", key="cf_src_windows")
        st.text_input("User", key="cf_src_user", disabled=windows_login)
        st.text_input("Password", type="password", key="cf_src_password", disabled=windows_login)
    with right:
        st.markdown("**PostgreSQL (target)**")
        st.text_input("Host", key="cf_tgt_host")
        st.text_input("Port", key="cf_tgt_port")
        st.text_input("Database ", key="cf_tgt_database")
        st.text_input("User ", key="cf_tgt_user")
        st.text_input("Password ", type="password", key="cf_tgt_password")
    if dev:
        with st.expander("Advanced (developer)"):
            installed = cf.sql_drivers()
            st.text_input("SQL Server ODBC driver", key="cf_src_driver",
                          help=f"Empty = the newest installed ({', '.join(installed) or 'none found'}).")
            st.checkbox("Trust the server certificate", key="cf_src_trust")

    if st.button("Test connection", type="primary", key="test_conn"):
        values = typed_values()
        problems = cf.validate(values)
        if problems:
            ss.conn_settings = ss.conn_sig = ss.conn_result = None
            st.error("\n\n".join(f"• {p}" for p in problems))
        else:
            settings = cf.build_settings(values)
            with st.spinner("Connecting to both databases ..."):
                result = cf.test_connection(settings, values)
            ss.conn_result = result
            if result["ok"]:
                ss.conn_settings, ss.conn_sig = settings, cf.signature(values)
                cf.save_last(values)
            else:
                ss.conn_settings = ss.conn_sig = None
            st.rerun()

    result = ss.conn_result
    if result:
        for label, key in (("SQL Server", "source"), ("PostgreSQL", "target")):
            ok, text = result[key]
            (st.success if ok else st.error)(f"**{label}:** {text}" if ok else f"**{label} failed.** {text}")
        if result["ok"] and result["tracking"] is False:
            st.warning("The target has no 'migration' schema yet, so only a rehearsal can run. Ask your DBA to "
                       "create it from sql/migration_schema.sql.")
        if result["ok"]:
            if settings_changed:
                st.warning("You changed the settings after the test. Press **Test connection** again.")
            else:
                st.info("Connected. Continue in the **🚀 Migrate** tab.")

# ----------------------------------------------------------------------------- migrate
with tab_migrate:
    if not connected:
        st.info("Open the **🔌 Database** tab, enter both databases and press **Test connection** first."
                if not dev else "No tested connection: using config/connections.yaml and .env, if present.")
    selected = selected_tables()
    errors = []
    if dev:
        st.markdown(view.step_heading(1, "Choose the tables"), unsafe_allow_html=True)
        b1, b2, b3, b4, _ = st.columns([1.3, 1, 1.7, 2.2, 3])
        b1.button("Select all", on_click=select_all)
        b2.button("Clear", on_click=clear_all)
        b3.button("Masters only", on_click=select_masters, help="Every table except the large autodata history.")
        b4.button("Add required tables", on_click=add_required,
                  help="Also select the tables that your selection depends on and that are not migrated yet.")

        widths = [0.5, 0.6, 3.2, 4.2, 3.0, 2.8, 1.2]
        header = st.columns(widths)
        for cell, label in zip(header, ["", "#", "Table", "Source → target", "Needs", "Status", "Rows"]):
            cell.markdown(f"<span style='font-size:.72rem;letter-spacing:.07em;text-transform:uppercase;"
                          f"font-weight:700;color:#64748b'>{label}</span>", unsafe_allow_html=True)

        for row in services.mapping_table_rows(ms, status):
            cells = st.columns(widths)
            cells[0].checkbox(f"select {row['name']}", key=f"sel_{row['name']}", label_visibility="collapsed")
            cells[1].write(row["order"])
            cells[2].markdown(f"**{row['name']}**" + ("  ⏱ windows" if row["windowed"] else ""))
            cells[3].caption(f"{row['source']}  \n→ {row['target']}")        # two short lines instead of one that wraps mid-word
            cells[4].caption(", ".join(row["needs"]) or "—")
            cells[5].markdown(view.pill((status.get(row["name"]) or {}).get("status") or "not_migrated"), unsafe_allow_html=True)
            cells[6].write("" if row["rows_loaded"] is None else f"{row['rows_loaded']:,}")

        selected = selected_tables()
        errors = validate_selection(selected, ms.tables, completed) if selected else []

        if not selected:
            st.info("Select at least one table.")
        elif errors:
            st.error("\n\n".join(f"• {e}" for e in errors))
        else:
            st.success(f"{len(selected)} table(s) selected.")
            st.caption("Order: " + " → ".join(plan_run(selected, ms.load_order)))

    st.markdown(view.step_heading(1 if not dev else 2, "How to run"), unsafe_allow_html=True)
    if dev:
        mode_label = st.radio("Mode", ["Rehearsal (keeps nothing)", "Real migration"], key="mode", horizontal=True)
        real = mode_label.startswith("Real")
    else:
        real = True                            # field mode has no rehearsal: it is always the real migration
        st.caption("All tables are migrated, in the right order. The data is kept in the target database.")

    start_dt = end_dt = None
    range_problem = None
    days = None
    max_windows = None
    resume = True
    if "autodata" in selected:
        st.markdown("**autodata (production history)**")
        low_day, high_day = source_range(ss.conn_sig) if (connected or dev) else (None, None)
        if dev:
            w1, w2, w3, w4 = st.columns(4)
        else:
            w1, w2, w3 = st.columns(3)
        limit = w1.checkbox("Limit the date range", key="limit_range",
                            help="Off = every day that has data in the source.")
        start_day = w2.date_input("From", value=low_day or date.today(), key="from_day", disabled=not limit)
        end_day = w3.date_input("To", value=high_day or date.today(), key="to_day", disabled=not limit)
        if low_day and high_day:
            st.caption(f"The source holds autodata from **{low_day}** to **{high_day}**. "
                       "From and To are logical days: From starts at the first shift, To ends with the last shift of that day.")
        if dev:
            days = int(w4.number_input("Days per window (0 = logical months)", min_value=0, max_value=365, value=0, key="days",
                                       help="0: one batch per logical month, cut at the logical-day start (recommended). "
                                            "A number: fixed windows of that many days."))
            w5, w6, _ = st.columns([2, 2, 4])
            max_windows = int(w5.number_input("Only the first N windows (0 = all)", min_value=0, max_value=5000,
                                              value=0 if real else 2, key="max_windows"))
            resume = w6.checkbox("Resume (skip finished windows)", value=True, key="resume", disabled=not real)
        else:
            max_windows = 0                       # every window
        if limit:
            start_dt, end_dt = datetime.combine(start_day, datetime.min.time()), datetime.combine(end_day, datetime.min.time())
            if end_day < start_day:
                range_problem = "The 'To' day is before the 'From' day."
            elif not services.range_has_data(start_day, end_day, low_day, high_day):
                range_problem = f"The source has no autodata between {start_day} and {end_day}."

    st.markdown(view.step_heading(2 if not dev else 3, "Review and start"), unsafe_allow_html=True)
    typed = ""
    if real:
        st.warning(f"A real migration changes the database **{info.get('target_database', '?')}**.")
        typed = st.text_input("Type the target database name to confirm", key="confirm_target")

    blockers = []
    if not connected and not dev:
        blockers.append("Test the database connection first (🔌 Database tab).")
    if settings_changed:
        blockers.append("The database settings changed after the test. Test them again.")
    if not selected:
        blockers.append("Select at least one table.")
    elif errors:
        blockers.append("Fix the selection errors above.")
    if range_problem:
        blockers.append(range_problem)
    if real and typed != info.get("target_database", None):
        blockers.append("Type the exact target database name.")
    if real and status_error:
        blockers.append("A real run needs the migration tracking schema.")
    if (connected or dev) and services.running_runs():
        blockers.append("A migration is already running.")
    process = st.session_state.get("proc")
    if (process is not None and process.poll() is None) or active_now:
        blockers.append("A run started from this page is still going.")

    st.markdown(f"{view.mode_badge('real' if real else 'rehearsal')} &nbsp; **{len(selected)}** table(s)",
                unsafe_allow_html=True)
    label = "Start migration" if real else "Start rehearsal"
    if st.button(label, type="primary", disabled=bool(blockers), key="start"):
        tables = None if selected == set(names) else plan_run(selected, ms.load_order)
        progress_path = services.new_progress_path()
        command = services.build_run_command(tables, "real" if real else "rehearsal", typed if real else None,
                                             start_dt, end_dt, days, max_windows, resume, progress_file=progress_path)
        process, out_path = services.launch_run(command, ss.conn_settings)
        st.session_state.update(run_options={"mode": "real" if real else "rehearsal", "tables": tables,
                                             "start": start_dt, "end": end_dt, "days": days,
                                             "max_windows": max_windows, "resume": resume},
                                package=None, package_key=None,proc=process, launch_out=out_path, progress_path=str(progress_path),
                                run_mode="real" if real else "rehearsal", was_active=True,
                                started_ts=datetime.now().timestamp())
        st.toast("Started. Watch it live right above.", icon="🚀")
        st.rerun()
    for reason in blockers:
        st.caption("• " + reason)

# ----------------------------------------------------------------------------- live progress
with tab_live:
    @st.fragment(run_every=1.0 if active_now else None)
    def live_tab():
        snap, active, launch_error = current_run()
        if snap is None:
            if launch_error:
                st.error("The run stopped before it started. Output:")
                st.code(launch_error, language="text")
            else:
                st.info("No run yet. Enter the databases, then press Start in the Migrate tab. "
                        "The progress, the stages and the row counts appear here live.")
            return
        process = st.session_state.get("proc")
        if process is not None and process.poll() is None:
            if st.button("Stop the run", key="stop"):
                process.terminate()
                problem = services.mark_running_as_stopped()
                st.warning("Stopped. The open transaction was rolled back by the database." + (f" {problem}" if problem else ""))

        st.markdown("#### Tables")
        st.markdown(view.table_rows(snap), unsafe_allow_html=True)

        run_id = snap.get("run_id")
        if dev and run_id and st.toggle("Show technical details (windows, events, log)", key="tech"):
            table_runs, _ = services.fetch_table_runs(run_id)
            if any(t["table_name"] == "autodata" for t in table_runs):
                windows, _ = services.fetch_windows(run_id, "autodata")
                if windows:
                    finished, total = services.window_progress(windows)
                    st.progress(finished / total if total else 0.0, text=f"autodata: {finished}/{total} windows")
                    st.dataframe(pd.DataFrame(windows), hide_index=True)
            events, _ = services.fetch_events(run_id, 50)
            if events:
                st.markdown("**Events**")
                st.dataframe(pd.DataFrame(events), hide_index=True)
            log_text = services.tail_log(run_id)
            if log_text:
                st.markdown("**Log (last lines)**")
                st.code(log_text, language="text")

        launch_out = st.session_state.get("launch_out")
        if dev and launch_out:
            with st.expander("Output of the last start from this page"):
                st.code(services.tail_text(launch_out) or "(nothing yet)", language="text")

    live_tab()

if not dev:
    st.stop()                                  # the tabs below exist in developer mode only

# ----------------------------------------------------------------------------- mapping
with tabs[3]:
    st.subheader("Table mapping")
    choice = st.selectbox("Table", names, key="mapping_choice")
    m = ms.tables[choice]
    st.markdown(f"`{(m.raw.get('source') or {}).get('schema', 'dbo')}.{m.src_table}`  →  "
                f"`{(m.raw.get('target') or {}).get('schema', '')}.{m.tgt_table}`")
    st.caption("Needs: " + (", ".join(sorted(set(m.depends_on))) or "nothing"))

    reviewed = get_reviewed().tables[choice]            # the reviewed mapping, without user choices
    saved = mo.load_override(choice)
    src_cols, src_err = services.fetch_source_columns(reviewed)
    tgt_cols, tgt_err = services.fetch_target_columns(reviewed)

    st.markdown("##### Reviewed mapping (cannot be changed here)")
    frame = pd.DataFrame([{"target column": c.dst, "source column": c.src or "", "rule": c.rule,
                           "details": json.dumps(c.params, default=str) if c.params else ""} for c in reviewed.columns])
    st.dataframe(frame, hide_index=True)

    if src_err or tgt_err:
        st.info((src_err or "") + " " + (tgt_err or "") + " Column types and your choices need both databases.")
    else:
        src_rows = mo.classify_source(reviewed, src_cols, saved)
        tgt_rows = mo.classify_target(reviewed, tgt_cols, saved)
        left_a, left_b = st.columns(2)
        with left_a:
            st.markdown("##### SQL Server columns")
            st.dataframe(pd.DataFrame([{"column": r["name"], "type": r["type_text"],
                                        "null": "NULL" if r["nullable"] else "NOT NULL",
                                        "status": r["status"], "note": r["note"]} for r in src_rows]), hide_index=True)
        with left_b:
            st.markdown("##### PostgreSQL columns")
            st.dataframe(pd.DataFrame([{"column": r["name"], "type": r["pg_type"],
                                        "null": "NULL" if r["nullable"] else "NOT NULL",
                                        "default": r["default"], "status": r["status"],
                                        "value comes from": r["how"]} for r in tgt_rows]), hide_index=True)

        open_src = [r for r in src_rows if r["status"] == mo.UNDECIDED]
        open_tgt = [r for r in tgt_rows if r["status"] in ("no value yet", "database default")]
        no_value = [r["name"] for r in tgt_rows if r["status"] == "no value yet" and not r["nullable"] and not r["default"]]
        if no_value:
            st.warning("NOT NULL target columns with no value yet (rows would go to the exceptions report): "
                       + ", ".join(no_value))

        st.markdown("##### Your choices (they only fill gaps)")
        src_by_name = {r["name"]: r for r in src_cols}
        draft = {"columns": list(saved.get("columns", [])), "leave_unmapped": list(saved.get("leave_unmapped", []))}
        tgt_names = [r["name"] for r in open_tgt]

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**A source column with no target**")
            if not open_src:
                st.caption("No undecided source columns.")
            else:
                pick = st.selectbox("Source column", [r["name"] for r in open_src], key=f"s_{choice}")
                what = st.radio("What to do", ["Map to a target column", "Leave it unmapped"], key=f"sw_{choice}")
                if what.startswith("Map"):
                    target = st.selectbox("Target column", tgt_names, key=f"st_{choice}")
                    if target and st.button("Add this mapping", key=f"sa_{choice}"):
                        verdict, rule, msg = mo.compatibility(src_by_name[pick]["type"], tgt_cols[target].pg_type)
                        choice_row = {"dst": target, "src": pick, "rule": rule or "copy"}
                        if rule == "cast":
                            choice_row["to"] = tgt_cols[target].pg_type
                        draft["columns"].append(choice_row)
                        problems = mo.validate_override(reviewed, draft, src_cols, tgt_cols)
                        if problems:
                            st.error("\n\n".join(problems))
                        else:
                            mo.save_override(choice, draft, "ui"); get_mappings.clear(); get_reviewed.clear()
                            st.success(msg or "Saved."); st.rerun()
                elif st.button("Leave it unmapped", key=f"sl_{choice}"):
                    draft["leave_unmapped"].append(pick)
                    mo.save_override(choice, draft, "ui"); get_mappings.clear(); get_reviewed.clear(); st.rerun()
        with c2:
            st.markdown("**A target column with no value**")
            if not tgt_names:
                st.caption("No open target columns.")
            else:
                target = st.selectbox("Target column", tgt_names, key=f"t_{choice}")
                kind = st.selectbox("Fill it with", ["Load time", "Constant", "NULL", "Database default",
                                                     "A source column"], key=f"tk_{choice}")
                new = {"dst": target}
                if kind == "Load time":
                    new["rule"] = "load_time"
                elif kind == "NULL":
                    new["rule"] = "set_null"
                elif kind == "Database default":
                    new["rule"] = "omit"
                elif kind == "Constant":
                    new["rule"] = "constant"
                    new["value"] = st.text_input("Value", key=f"tv_{choice}")
                else:
                    options = [r["name"] for r in open_src]
                    source = st.selectbox("Source column", options, key=f"ts_{choice}") if options else None
                    if source:
                        verdict, rule, _ = mo.compatibility(src_by_name[source]["type"], tgt_cols[target].pg_type)
                        new.update({"rule": rule or "copy", "src": source})
                        if rule == "cast":
                            new["to"] = tgt_cols[target].pg_type
                if st.button("Add this value", key=f"ta_{choice}") and "rule" in new:
                    draft["columns"].append(new)
                    problems = mo.validate_override(reviewed, draft, src_cols, tgt_cols)
                    if problems:
                        st.error("\n\n".join(problems))
                    else:
                        mo.save_override(choice, draft, "ui"); get_mappings.clear(); get_reviewed.clear(); st.rerun()

        if saved.get("columns") or saved.get("leave_unmapped"):
            st.markdown("**Saved choices for this table**")
            items = [f"{c['dst']} ← {mo.describe_rule(c)}" for c in saved.get("columns", [])]
            items += [f"{n}: left unmapped" for n in saved.get("leave_unmapped", [])]
            for item in items:
                st.markdown(f"- {item}")
            if st.button("Reset my choices for this table", key=f"reset_{choice}"):
                mo.save_override(choice, {}, "ui"); get_mappings.clear(); get_reviewed.clear(); st.rerun()

    if m.default:
        st.markdown("**Filled by the database (not written by the migration)**")
        st.json({k: str(v) for k, v in m.default.items()})
    if m.unmapped_src:
        st.markdown("**Source columns that are not migrated**")
        st.write(", ".join(str(c) for c in m.unmapped_src))
    decisions = m.raw.get("decisions") or []
    if decisions:
        st.markdown("**Decisions recorded for this table**")
        for item in decisions:
            st.markdown(f"- {item}")

# ----------------------------------------------------------------------------- history
with tabs[4]:
    st.subheader("Past runs")
    runs, error = services.fetch_runs(100)
    if error and not runs:
        st.info(error)
    if runs:
        st.dataframe(pd.DataFrame(runs), hide_index=True)
        run_id = st.selectbox("Run", [r["run_id"] for r in runs], key="history_run")
        table_runs, _ = services.fetch_table_runs(run_id)
        if table_runs:
            st.dataframe(pd.DataFrame(table_runs).drop(columns=["exceptions_file"]), hide_index=True)
            for t in table_runs:
                data = services.read_exceptions_file(t.get("exceptions_file"))
                if data:
                    st.download_button(f"Download exceptions of {t['table_name']}", data,
                                       file_name=f"{t['table_name']}_exceptions.csv", key=f"dl_{run_id}_{t['table_name']}")
