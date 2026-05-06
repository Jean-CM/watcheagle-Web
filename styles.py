BASE_CSS = """
* { box-sizing:border-box; }
body { margin:0; font-family:Arial, sans-serif; background:#061126; color:#e5e7eb; padding:24px; }
.page { max-width:1750px; margin:0 auto; }
h1 { margin:0; font-size:34px; font-weight:900; color:#fff; }
.subtitle { color:#9fb0c8; margin:6px 0 18px; }
.nav,.tools { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:16px; }
.nav-link,.tool-link { text-decoration:none; color:#d8b4fe; background:rgba(168,85,247,.09); border:1px solid rgba(168,85,247,.35); padding:10px 14px; border-radius:12px; font-weight:800; font-size:14px; }
.nav-link.active { background:rgba(59,130,246,.18); color:#bfdbfe; border-color:rgba(96,165,250,.65); }
.tool-link { color:#c7d2fe; background:rgba(99,102,241,.08); border-color:rgba(99,102,241,.25); font-size:13px; }
.grid { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:18px; }
.grid-3 { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:18px; }
.grid-2 { display:grid; grid-template-columns:repeat(2,1fr); gap:18px; margin-bottom:18px; }
.card { background:linear-gradient(180deg,rgba(18,32,60,.96),rgba(13,24,47,.98)); border:1px solid rgba(96,165,250,.16); border-radius:18px; padding:18px; box-shadow:0 10px 28px rgba(0,0,0,.24); }
.label { color:#9fb0c8; font-size:13px; margin-bottom:10px; }
.value { font-size:34px; font-weight:900; }
.section-title { font-size:21px; font-weight:900; margin:0 0 12px; color:#f8fafc; }
table { width:100%; border-collapse:collapse; background:#0d172c; border:1px solid rgba(96,165,250,.16); border-radius:18px; overflow:hidden; margin-bottom:18px; }
th { background:rgba(30,41,72,.94); color:#f8fafc; padding:14px 12px; font-size:14px; text-align:left; }
td { padding:13px 12px; border-top:1px solid rgba(51,65,85,.75); font-size:14px; vertical-align:top; }
tr:hover td { background:rgba(255,255,255,.025); }
.badge { display:inline-block; padding:5px 10px; border-radius:999px; font-size:12px; font-weight:900; border:1px solid currentColor; }
.ok { color:#22c55e; background:rgba(34,197,94,.14); }
.warn { color:#f59e0b; background:rgba(245,158,11,.14); }
.incident { color:#ef4444; background:rgba(239,68,68,.14); }
.pending { color:#94a3b8; background:rgba(148,163,184,.12); }
.green { color:#34d399; } .red { color:#ef4444; } .yellow { color:#f59e0b; } .blue { color:#60a5fa; } .muted { color:#94a3b8; }
.field { display:flex; flex-direction:column; gap:6px; }
.field label { color:#9fb0c8; font-size:12px; font-weight:800; }
input,select,textarea { background:#0b1730; color:white; border:1px solid rgba(96,165,250,.24); border-radius:12px; padding:12px; width:100%; }
textarea { min-height:95px; }
.form-grid { display:grid; grid-template-columns:1fr 1fr 1fr auto; gap:12px; align-items:end; }
.btn { border:0; border-radius:12px; padding:11px 14px; font-weight:900; color:white; text-decoration:none; display:inline-block; cursor:pointer; }
.btn-primary { background:linear-gradient(135deg,#2563eb,#7c3aed); }
.btn-danger { background:linear-gradient(135deg,#dc2626,#991b1b); }
.btn-secondary { background:linear-gradient(135deg,#334155,#1e293b); }
.actions { display:flex; flex-wrap:wrap; gap:8px; }
.mini-row { display:flex; justify-content:space-between; border-bottom:1px solid rgba(51,65,85,.7); padding:10px 0; gap:12px; }
.mini-row:last-child { border-bottom:0; }
canvas { width:100% !important; max-height:330px; }
pre { background:#020617; color:#d1d5db; border:1px solid rgba(96,165,250,.24); border-radius:16px; padding:18px; white-space:pre-wrap; }
@media(max-width:1100px) { .grid,.grid-3,.grid-2,.form-grid { grid-template-columns:1fr; } }
"""
