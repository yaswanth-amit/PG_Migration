"""Streamlit page for the migration.   Start it with:   streamlit run ui_app.py

The page only READS the migration.* tables and the run's progress file, and STARTS main.py as a separate
process, so closing the browser never interrupts a run. Nothing here shows row values, only names, counts
and times.
"""
import json
from datetime import date, datetime

import pandas as pd
import streamlit as st

from pipeline import services, ui_components as ui
from pipeline.mapping_config import load_mappings
from pipeline.planner import plan_run, validate_selection, with_dependencies

st.set_page_config(page_title="TPM Data Migration", page_icon="🚀", layout="wide")
st.markdown(ui.CSS, unsafe_allow_html=True)


@st.cache_resource
def get_mappings():
    return load_mappings()


@st.cache_data(ttl=300, show_spinner=False)
def source_range():
    return services.fetch_source_range(get_mappings().tables["autodata"])


ms = get_mappings()
names = list(ms.load_order)
status, status_error = services.fetch_status()
completed = {n for n, row in status.items() if row.get("status") in services.DONE_STATUSES}
info = services.connection_info()


def selected_tables() -> set[str]:
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
    st.markdown(ui.live_card(snapshot, summary, previous, detail), unsafe_allow_html=True)
    return summary


snapshot_now, active_now, _ = current_run()


# ----------------------------------------------------------------------------- header and sidebar
st.markdown(ui.hero(info), unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Run details")
    st.markdown(ui.side_card("Source (SQL Server)", info.get("source", "unknown")), unsafe_allow_html=True)
    st.markdown(ui.side_card("Target (PostgreSQL)", info.get("target", "unknown")), unsafe_allow_html=True)
    if status_error:
        st.warning(status_error)
    st.markdown("### Pipeline")
    st.markdown(ui.stage_track("", services.TABLE_STAGES, None, vertical=True), unsafe_allow_html=True)
    st.caption("Each table is **extracted** from SQL Server, **transformed** by its mapping, **loaded** into "
               "PostgreSQL and then **recorded** with its counts.")
    st.markdown("---")
    st.caption("A rehearsal runs everything and rolls it back. A real migration keeps the data, so it needs "
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

tab_migrate, tab_live, tab_mapping, tab_history = st.tabs(["🚀  Migrate", "📡  Live progress", "🗺️  Mapping", "🕘  History"])

# ----------------------------------------------------------------------------- migrate
with tab_migrate:
    st.markdown(ui.step_heading(1, "Choose the tables"), unsafe_allow_html=True)
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
        cells[5].markdown(ui.pill((status.get(row["name"]) or {}).get("status") or "not_migrated"), unsafe_allow_html=True)
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

    st.markdown(ui.step_heading(2, "How to run"), unsafe_allow_html=True)
    mode_label = st.radio("Mode", ["Rehearsal (keeps nothing)", "Real migration"], key="mode", horizontal=True)
    real = mode_label.startswith("Real")

    start_dt = end_dt = None
    range_problem = None
    days = max_windows = None
    resume = True
    if "autodata" in selected:
        st.markdown("**autodata windows**")
        low_day, high_day = source_range()
        w1, w2, w3, w4 = st.columns(4)
        limit = w1.checkbox("Limit the date range", key="limit_range",
                            help="Off = every day that has data in the source.")
        start_day = w2.date_input("From", value=low_day or date.today(), key="from_day", disabled=not limit)
        end_day = w3.date_input("To", value=high_day or date.today(), key="to_day", disabled=not limit)
        if low_day and high_day:
            st.caption(f"The source holds autodata from **{low_day}** to **{high_day}**.")
        days = int(w4.number_input("Days per window", min_value=1, max_value=365, value=15, key="days"))
        w5, w6, _ = st.columns([2, 2, 4])
        max_windows = int(w5.number_input("Only the first N windows (0 = all)", min_value=0, max_value=5000,
                                          value=0 if real else 2, key="max_windows"))
        resume = w6.checkbox("Resume (skip finished windows)", value=True, key="resume", disabled=not real)
        if limit:
            start_dt, end_dt = datetime.combine(start_day, datetime.min.time()), datetime.combine(end_day, datetime.min.time())
            if end_day < start_day:
                range_problem = "The 'To' day is before the 'From' day."
            elif not services.range_has_data(start_day, end_day, low_day, high_day):
                range_problem = f"The source has no autodata between {start_day} and {end_day}."

    st.markdown(ui.step_heading(3, "Review and start"), unsafe_allow_html=True)
    typed = ""
    if real:
        st.warning(f"A real migration changes the database **{info.get('target_database', '?')}**.")
        typed = st.text_input("Type the target database name to confirm", key="confirm_target")

    blockers = []
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
    if services.running_runs():
        blockers.append("A migration is already running.")
    process = st.session_state.get("proc")
    if (process is not None and process.poll() is None) or active_now:
        blockers.append("A run started from this page is still going.")

    st.markdown(f"{ui.mode_badge('real' if real else 'rehearsal')} &nbsp; **{len(selected)}** table(s)",
                unsafe_allow_html=True)
    label = "Start migration" if real else "Start rehearsal"
    if st.button(label, type="primary", disabled=bool(blockers), key="start"):
        tables = None if selected == set(names) else plan_run(selected, ms.load_order)
        progress_path = services.new_progress_path()
        command = services.build_run_command(tables, "real" if real else "rehearsal", typed if real else None,
                                             start_dt, end_dt, days, max_windows, resume, progress_file=progress_path)
        process, out_path = services.launch_run(command)
        st.session_state.update(proc=process, launch_out=out_path, progress_path=str(progress_path),
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
                st.info("No run yet. Choose tables in the Migrate tab and press Start. "
                        "The progress, the stages and the row counts appear here live.")
            return
        process = st.session_state.get("proc")
        if process is not None and process.poll() is None:
            if st.button("Stop the run", key="stop"):
                process.terminate()
                problem = services.mark_running_as_stopped()
                st.warning("Stopped. The open transaction was rolled back by the database." + (f" {problem}" if problem else ""))

        st.markdown("#### Tables")
        st.markdown(ui.table_rows(snap), unsafe_allow_html=True)

        run_id = snap.get("run_id")
        if run_id and st.toggle("Show technical details (windows, events, log)", key="tech"):
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
        if launch_out:
            with st.expander("Output of the last start from this page"):
                st.code(services.tail_text(launch_out) or "(nothing yet)", language="text")

    live_tab()

# ----------------------------------------------------------------------------- mapping
with tab_mapping:
    st.subheader("Table mapping")
    choice = st.selectbox("Table", names, key="mapping_choice")
    m = ms.tables[choice]
    st.markdown(f"`{(m.raw.get('source') or {}).get('schema', 'dbo')}.{m.src_table}`  →  "
                f"`{(m.raw.get('target') or {}).get('schema', '')}.{m.tgt_table}`")
    st.caption("Needs: " + (", ".join(sorted(set(m.depends_on))) or "nothing"))

    frame = pd.DataFrame([{"target column": c.dst, "source column": c.src or "", "rule": c.rule,
                           "details": json.dumps(c.params, default=str) if c.params else ""} for c in m.columns])
    st.dataframe(frame, hide_index=True)

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
with tab_history:
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
