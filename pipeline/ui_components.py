"""HTML and CSS for the web page: plain functions that return strings, so they are tested without a browser.
Only names, counts and times are drawn, never row values. Everything dynamic goes through html.escape.
Markdown treats a line indented by 4+ spaces as code, so every block here is built without indentation.
"""
from html import escape

from pipeline import services

CSS = """
<style>
:root{--ac:#4f46e5;--ac2:#06b6d4;--ok:#16a34a;--warn:#d97706;--bad:#dc2626;--mut:rgba(100,116,139,.95);--line:rgba(100,116,139,.22);--card:rgba(148,163,184,.10)}
.block-container{padding-top:1.4rem;max-width:1280px}
.hero{background:linear-gradient(120deg,#312e81 0%,#4f46e5 45%,#06b6d4 100%);color:#fff;border-radius:18px;padding:26px 30px;margin-bottom:18px;box-shadow:0 10px 30px rgba(79,70,229,.25);position:relative;overflow:hidden}
.hero:after{content:"";position:absolute;right:-60px;top:-60px;width:240px;height:240px;border-radius:50%;background:rgba(255,255,255,.08)}
.hero h1{margin:0;font-size:1.9rem;font-weight:800;letter-spacing:-.02em;color:#fff}
.hero p{margin:4px 0 14px;opacity:.88;font-size:1rem}
.chips{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.chip{background:rgba(255,255,255,.16);border:1px solid rgba(255,255,255,.28);border-radius:999px;padding:5px 14px;font-size:.82rem;backdrop-filter:blur(4px)}
.chip b{opacity:.75;font-weight:600;margin-right:6px;text-transform:uppercase;font-size:.7rem;letter-spacing:.06em}
.arrow{font-size:1.3rem;opacity:.9}
.step{display:flex;align-items:center;gap:10px;margin:22px 0 8px;font-weight:700;font-size:1.12rem}
.step i{font-style:normal;width:28px;height:28px;border-radius:50%;background:var(--ac);color:#fff;display:inline-flex;align-items:center;justify-content:center;font-size:.85rem}
.pill{display:inline-block;padding:3px 11px;border-radius:999px;font-size:.76rem;font-weight:600;white-space:nowrap}
.track.col{flex-direction:column;align-items:flex-start;gap:0;margin:6px 0}
.track.col .link{width:3px;height:14px;margin:2px 0 2px 9px}
.p-pending,.p-not_migrated{background:rgba(100,116,139,.15);color:var(--mut)}
.p-running{background:rgba(79,70,229,.14);color:var(--ac)}
.p-completed{background:rgba(22,163,74,.14);color:var(--ok)}
.p-completed_with_exceptions{background:rgba(217,119,6,.15);color:var(--warn)}
.p-failed,.p-blocked,.p-cancelled{background:rgba(220,38,38,.14);color:var(--bad)}
.mode{display:inline-block;padding:4px 12px;border-radius:8px;font-weight:700;font-size:.8rem}
.mode-rehearsal{background:rgba(22,163,74,.14);color:var(--ok)}
.mode-real{background:rgba(220,38,38,.14);color:var(--bad)}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:20px 22px;margin:8px 0 14px}
.live{border-left:6px solid var(--ac)}
.live.done{border-left-color:var(--ok)}.live.warn{border-left-color:var(--warn)}.live.bad{border-left-color:var(--bad)}
.live-head{display:flex;align-items:center;gap:16px;margin-bottom:14px}
.live-title{font-size:1.25rem;font-weight:800;letter-spacing:-.01em}
.live-sub{color:var(--mut);font-size:.88rem;margin-top:2px}
.spin{width:38px;height:38px;border-radius:50%;border:4px solid rgba(79,70,229,.18);border-top-color:var(--ac);border-right-color:var(--ac2);animation:sp .9s linear infinite;flex:none}
@keyframes sp{to{transform:rotate(360deg)}}
.icon{width:38px;height:38px;border-radius:50%;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:1.2rem;flex:none;animation:pop .45s ease-out}
.icon.ok{background:var(--ok)}.icon.warn{background:var(--warn)}.icon.bad{background:var(--bad)}
@keyframes pop{0%{transform:scale(.3);opacity:0}70%{transform:scale(1.15)}100%{transform:scale(1);opacity:1}}
.badge-live{margin-left:auto;background:rgba(79,70,229,.12);color:var(--ac);border-radius:999px;padding:3px 12px;font-size:.72rem;font-weight:800;letter-spacing:.08em;display:flex;align-items:center;gap:6px}
.badge-live:before{content:"";width:8px;height:8px;border-radius:50%;background:var(--ac);animation:pulse 1.2s ease-in-out infinite}
@keyframes pulse{50%{opacity:.25;transform:scale(.7)}}
.bar{height:16px;border-radius:999px;background:rgba(100,116,139,.18);overflow:hidden}
.bar-fill{height:100%;border-radius:999px;width:var(--to);background:linear-gradient(90deg,var(--ac),var(--ac2));animation:grow 1.2s ease-out}
.bar-fill.run{background-image:repeating-linear-gradient(135deg,rgba(255,255,255,.28) 0 12px,transparent 12px 24px),linear-gradient(90deg,var(--ac),var(--ac2));background-size:34px 34px,100% 100%;animation:grow 1.2s ease-out,stripes 1s linear infinite}
.bar-fill.ok{background:var(--ok)}.bar-fill.warn{background:var(--warn)}.bar-fill.bad{background:var(--bad)}
@keyframes grow{from{width:var(--from)}to{width:var(--to)}}
@keyframes stripes{to{background-position:34px 0,0 0}}
.bar-meta{display:flex;justify-content:space-between;font-size:.85rem;color:var(--mut);margin:6px 2px 0;font-weight:600}
.big{font-size:2.5rem;font-weight:800;letter-spacing:-.03em;line-height:1.1;margin:10px 0 2px}
.big small{font-size:1rem;font-weight:600;color:var(--mut);letter-spacing:0}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(118px,1fr));gap:12px;margin-top:16px}
.tile{background:rgba(148,163,184,.10);border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.tile span{display:block;color:var(--mut);font-size:.72rem;text-transform:uppercase;letter-spacing:.07em;font-weight:700}
.tile b{font-size:1.35rem;letter-spacing:-.01em}
.track{display:flex;align-items:center;margin:16px 0 4px;flex-wrap:wrap;gap:4px 0}
.track .lbl{overflow-wrap:anywhere;font-size:.72rem;text-transform:uppercase;letter-spacing:.07em;font-weight:700;color:var(--mut);margin-right:12px;min-width:170px}
.node{display:flex;align-items:center;gap:7px;font-size:.84rem;font-weight:600;color:var(--mut)}
.node i{font-style:normal;width:22px;height:22px;border-radius:50%;background:rgba(100,116,139,.2);display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;color:var(--mut)}
.node.done{color:var(--ok)}.node.done i{background:var(--ok);color:#fff}
.node.now{color:var(--ac)}.node.now i{background:var(--ac);color:#fff;animation:ring 1.4s ease-out infinite}
@keyframes ring{0%{box-shadow:0 0 0 0 rgba(79,70,229,.55)}100%{box-shadow:0 0 0 10px rgba(79,70,229,0)}}
.link{width:28px;height:3px;background:rgba(100,116,139,.25);margin:0 8px;border-radius:2px}
.link.done{background:var(--ok)}
.trows{max-height:460px;overflow-y:auto;border:1px solid var(--line);border-radius:14px}
.trow{display:grid;grid-template-columns:2.2fr 1.5fr 3fr 1.6fr;gap:12px;align-items:center;padding:10px 16px;border-bottom:1px solid var(--line);font-size:.9rem}
.trow:last-child{border-bottom:0}
.trow.now{background:rgba(79,70,229,.07)}
.trow .nm{font-weight:700}.trow .nm small{display:block;color:var(--mut);font-weight:500;font-size:.74rem}
.trow .num{text-align:right;font-variant-numeric:tabular-nums;color:var(--mut)}
.trow .num b{color:inherit;opacity:1}
.mini{height:8px;border-radius:99px;background:rgba(100,116,139,.18);overflow:hidden}
.mini div{height:100%;border-radius:99px;background:linear-gradient(90deg,var(--ac),var(--ac2));width:var(--to);animation:grow 1s ease-out}
.mini div.ok{background:var(--ok)}.mini div.warn{background:var(--warn)}.mini div.bad{background:var(--bad)}
.thead{font-size:.72rem;text-transform:uppercase;letter-spacing:.07em;font-weight:700;color:var(--mut);background:rgba(148,163,184,.12)}
.err{color:var(--bad);font-size:.78rem}
.side-card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 14px;margin-bottom:10px;font-size:.85rem}
.side-card span{display:block;color:var(--mut);font-size:.68rem;text-transform:uppercase;letter-spacing:.07em;font-weight:700}
.stTabs [data-baseweb="tab"]{font-weight:700;padding:10px 18px}
div.stButton>button[kind="primary"]{background:linear-gradient(90deg,var(--ac),var(--ac2));border:0;font-weight:700;padding:.6rem 1.6rem;border-radius:12px;box-shadow:0 6px 16px rgba(79,70,229,.3);transition:transform .15s}
div.stButton>button[kind="primary"]:hover{transform:translateY(-2px)}
div.stButton>button[kind="primary"]:disabled{background:rgba(100,116,139,.3);box-shadow:none}
.rowline{padding:6px 0;border-bottom:1px solid var(--line)}
</style>
"""

STATUS_TEXT = {"pending": "Waiting", "running": "In progress", "completed": "Migrated",
               "completed_with_exceptions": "Migrated · some held back", "failed": "Failed",
               "blocked": "Blocked", "cancelled": "Cancelled", "not_migrated": "Not migrated"}


def pill(status : str) -> str:
    return f'<span class="pill p-{escape(status)}">{escape(STATUS_TEXT.get(status, status))}</span>'


def mode_badge(mode : str) -> str:
    real = mode == "real"
    return f'<span class="mode mode-{"real" if real else "rehearsal"}">{"REAL MIGRATION" if real else "REHEARSAL"}</span>'


def hero(info : dict) -> str:
    return ('<div class="hero"><h1>TPM Data Migration</h1>'
            '<p>Move your master data and production history from SQL Server to PostgreSQL, safely and visibly.</p>'
            '<div class="chips">'
            f'<span class="chip"><b>Source</b>{escape(info.get("source", "unknown"))}</span>'
            '<span class="arrow">➜</span>'
            f'<span class="chip"><b>Target</b>{escape(info.get("target", "unknown"))}</span>'
            '</div></div>')


def step_heading(number : int , text : str) -> str:
    return f'<div class="step"><i>{number}</i>{escape(text)}</div>'


def side_card(label : str , value : str) -> str:
    return f'<div class="side-card"><span>{escape(label)}</span>{escape(value)}</div>'


def stage_track(label : str , stages : list[tuple[str, str]] , current : str | None , all_done : bool = False ,
                vertical : bool = False) -> str:
    """A row of numbered steps: finished ones are green, the current one pulses."""
    keys = [k for k, _ in stages]
    at = keys.index(current) if current in keys else (len(keys) if all_done else -1)
    parts = [f'<span class="lbl">{escape(label)}</span>'] if label else []
    for i, (key, text) in enumerate(stages):
        state = "done" if i < at else ("now" if i == at else "")
        mark = "✓" if state == "done" else str(i + 1)
        if i:
            parts.append(f'<span class="link {"done" if i <= at else ""}"></span>')
        parts.append(f'<span class="node {state}"><i>{mark}</i>{escape(text)}</span>')
    return f'<div class="track{" col" if vertical else ""}">{"".join(parts)}</div>'


def _tone(snapshot : dict , summary : dict) -> str:
    status = snapshot.get("status")
    if status in ("failed", "cancelled") or summary["tables_failed"]:
        return "bad"
    if status == "completed_with_exceptions" or summary["rows_excepted"]:
        return "warn"
    return "ok" if status == "completed" else ""


def live_card(snapshot : dict , summary : dict , prev_pct : float = 0.0 , detail : bool = True) -> str:
    """The big progress card: loader, rows migrated, stages and numbers."""
    running = snapshot.get("status") in ("running", "starting")
    tone = _tone(snapshot, summary)
    rehearsal = snapshot.get("mode") != "real"
    pct = summary["pct"]

    if running:
        icon = '<div class="spin"></div>'
        phase = summary["phase"]
        if summary["active_table"]:
            title = f'Migrating {escape(summary["active_table"])}'
        else:
            title = {"preparing": "Preparing the run…", "counting": "Counting the source rows…",
                     "finalizing": "Finalizing…"}.get(phase, "Working…")
        badge = '<span class="badge-live">LIVE</span>'
    else:
        status = snapshot.get("status")
        glyph = {"bad": "✕", "warn": "!", "ok": "✓"}.get(tone, "✓")
        icon = f'<div class="icon {tone or "ok"}">{glyph}</div>'
        title = {"completed": "Rehearsal complete" if rehearsal else "Migration complete",
                 "completed_with_exceptions": ("Rehearsal complete" if rehearsal else "Migration complete") + " · some rows held back",
                 "failed": "Run finished with failures", "cancelled": "Run was stopped"}.get(status, "Run finished")
        badge = ""
    sub = f'{mode_badge(snapshot.get("mode", "rehearsal"))} &nbsp; elapsed {services.fmt_duration(summary["elapsed"])}'
    if running and summary["eta"] is not None:
        sub += f' &nbsp;·&nbsp; about {services.fmt_duration(summary["eta"])} left'
    if not running and rehearsal and tone != "bad":
        sub += " &nbsp;·&nbsp; everything was rolled back, nothing was kept"

    fill_class = "run" if running else tone
    html = [f'<div class="card live {"" if running else ("done" if tone == "ok" else tone)}">',
            f'<div class="live-head">{icon}<div><div class="live-title">{title}</div><div class="live-sub">{sub}</div></div>{badge}</div>']

    total = summary["rows_total"]
    big = f'{services.fmt_int(summary["rows_loaded"])}'
    of = f' <small>of {services.fmt_int(total)} rows migrated</small>' if total else ' <small>rows migrated</small>'
    html.append(f'<div class="big">{big}{of}</div>')
    html.append(f'<div class="bar"><div class="bar-fill {fill_class}" style="--from:{prev_pct * 100:.1f}%;--to:{pct * 100:.1f}%"></div></div>')
    html.append(f'<div class="bar-meta"><span>{pct * 100:.1f}%</span>'
                f'<span>{summary["tables_done"]} of {summary["tables_total"]} tables</span></div>')

    if detail:
        phase = summary["phase"]
        html.append(stage_track("Run", services.RUN_PHASES, phase if running else None, all_done=not running))
        if running and summary["active_table"]:
            html.append(stage_track(summary["active_table"],services.TABLE_STAGES, summary["active_stage"]))
        html.append('<div class="tiles">'
                    f'<div class="tile"><span>Rows read</span><b>{services.fmt_int(summary["rows_read"])}</b></div>'
                    f'<div class="tile"><span>Rows migrated</span><b>{services.fmt_int(summary["rows_loaded"])}</b></div>'
                    f'<div class="tile"><span>Held back</span><b>{services.fmt_int(summary["rows_excepted"])}</b></div>'
                    f'<div class="tile"><span>Speed</span><b>{services.fmt_int(summary["rate"])}/s</b></div>'
                    f'<div class="tile"><span>Tables</span><b>{summary["tables_done"]}/{summary["tables_total"]}</b></div>'
                    '</div>')
    html.append("</div>")
    return "".join(html)


def table_rows(snapshot : dict) -> str:
    """One line per table: name, status, a small bar and its row counts."""
    rows = ['<div class="trows"><div class="trow thead"><span>Table</span><span>Status</span><span>Progress</span>'
            '<span style="text-align:right">Rows</span></div>']
    for t in snapshot.get("tables", []):
        status = t.get("status", "pending")
        total, read = t.get("rows_total"), t.get("rows_read", 0)
        if status in services.DONE_STATUSES:
            pct = 1.0
        elif total:
            pct = min(read / total, 1.0)
        else:
            pct = 0.0
        tone = {"completed": "ok", "completed_with_exceptions": "warn", "failed": "bad", "blocked": "bad"}.get(status, "")
        stage = dict(services.TABLE_STAGES).get(t.get("stage") or "", "")
        sub = f"{escape(stage)}…" if status == "running" and stage else ""
        if t.get("windows_total"):
            sub = (sub + " · " if sub else "") + f'window {t.get("windows_done", 0)}/{t["windows_total"]}'
        count = (f'<b>{services.fmt_int(t.get("rows_loaded", 0))}</b>' + (f' / {services.fmt_int(total)}' if total else ""))
        err = f'<div class="err">{escape(str(t["error"])[:160])}</div>' if t.get("error") else ""
        if status in services.DONE_STATUSES and read == 0 and t.get("windows_total"):
            err = ('<div class="err" style="color:var(--warn)">No rows were read. Check the date range, or run again '
                   'without Resume if the target was emptied.</div>')
        rows.append(f'<div class="trow {"now" if status == "running" else ""}">'
                    f'<span class="nm">{escape(t["name"])}<small>{sub}</small></span>{"<span>" + pill(status) + "</span>"}'
                    f'<span><div class="mini"><div class="{tone}" style="--from:{pct * 100:.1f}%;--to:{pct * 100:.1f}%"></div></div>{err}</span>'
                    f'<span class="num">{count}</span></div>')
    rows.append("</div>")
    return "".join(rows)
