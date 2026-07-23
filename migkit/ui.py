import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import REPORTS, load_hops

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit</title>
<style>
:root { color-scheme: light dark; }
* { box-sizing: border-box; margin: 0; }
body { font: 14px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif;
  background: #f4f4f2; color: #0b0b0b; padding: 28px 16px; }
.wrap { max-width: 1100px; margin: 0 auto; }
h1 { font-size: 18px; font-weight: 600; margin-bottom: 4px; }
.sub { color: #52514e; font-size: 12.5px; margin-bottom: 18px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
  gap: 14px; }
.card { background: #fcfcfb; border: 1px solid #e4e3df; border-radius: 6px;
  padding: 16px 18px; }
.card h2 { font-size: 14px; font-weight: 600; }
.route { font-family: ui-monospace, Menlo, monospace; font-size: 11px;
  color: #52514e; margin: 2px 0 10px; word-break: break-all; }
.tiles { display: flex; gap: 8px; margin-bottom: 10px; }
.tile { flex: 1; border: 1px solid #eceae6; border-radius: 6px;
  padding: 6px 8px; text-align: center; }
.tile b { font-size: 16px; display: block; }
.tile span { font-size: 10.5px; color: #52514e; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
  margin-right: 5px; }
.ok { background: #0ca30c; } .diff { background: #fab219; }
.error { background: #d03b3b; } .na { background: #c3c2b7; }
.rows { font-size: 12px; color: #52514e; }
.rows div { padding: 2px 0; border-top: 1px solid #f1f0ec; }
a { color: inherit; }
.meta { font-size: 11px; color: #52514e; margin-top: 8px; }
footer { color: #52514e; font-size: 11px; text-align: center; margin-top: 20px; }
@media (prefers-color-scheme: dark) {
  body { background: #111110; color: #fff; }
  .card { background: #1a1a19; border-color: #33322f; }
  .tile { border-color: #2a2927; }
  .rows div { border-color: #242422; }
}
</style></head><body><div class="wrap">
<h1>migkit</h1>
<div class="sub" id="ts">loading...</div>
<div class="grid" id="grid"></div>
<footer>auto-refresh 10s, read-only view</footer>
</div>
<script>
async function load() {
  const r = await fetch('/api/data');
  const data = await r.json();
  document.getElementById('ts').textContent =
    data.hops.length + ' hops, updated ' + data.now;
  const g = document.getElementById('grid');
  g.innerHTML = '';
  for (const h of data.hops) {
    const el = document.createElement('div');
    el.className = 'card';
    let tiles = '';
    for (const [name, st] of Object.entries(h.checks)) {
      tiles += `<div class="tile"><b><span class="dot ${st.status}"></span>` +
        `${st.pass}/${st.total}</b><span>${name}</span></div>`;
    }
    let rows = '';
    for (const d of h.dbs) {
      rows += `<div><span class="dot ${d.status}"></span>${d.name}` +
        (d.note ? ` <span style="opacity:.7">${d.note}</span>` : '') + '</div>';
    }
    el.innerHTML =
      `<h2><span class="dot ${h.status}"></span>${h.name}` +
      (h.has_report ? ` <a href="/report/${h.name}" target="_blank"` +
        ` style="font-size:11px">report</a>` : '') + `</h2>` +
      `<div class="route">${h.route}</div>` +
      (tiles ? `<div class="tiles">${tiles}</div>` : '') +
      `<div class="rows">${rows}</div>` +
      `<div class="meta">${h.meta}</div>`;
    g.appendChild(el);
  }
}
load();
setInterval(load, 10000);
</script></body></html>"""


def _hop_data(name, hop):
    out = {"name": name, "status": "na", "checks": {}, "dbs": [],
           "meta": "no checks run yet", "has_report": False,
           "route": f"{hop.source.host or '?'} -> "
                    f"{hop.target.host or hop.target.options.get('hosts', '?')}"}
    rpt = REPORTS / name
    summary = rpt / "summary.json"
    out["has_report"] = (rpt / "report.html").exists()
    if summary.exists():
        try:
            results = json.loads(summary.read_text())
        except ValueError:
            results = []
        per_check = {}
        per_db = {}
        for r in results:
            c = r.get("check", "?")
            db = r.get("scope", "?").split()[0].split(".")[0]
            st = r.get("status", "?")
            pc = per_check.setdefault(c, {"pass": 0, "total": 0,
                                          "status": "ok"})
            pc["total"] += 1
            if st == "ok":
                pc["pass"] += 1
            elif st == "error":
                pc["status"] = "error"
            elif pc["status"] != "error":
                pc["status"] = "diff"
            cur = per_db.setdefault(db, {"status": "ok", "note": ""})
            if st == "error":
                cur["status"] = "error"
            elif st == "diff" and cur["status"] != "error":
                cur["status"] = "diff"
                cur["note"] = (r.get("detail") or "")[:60]
        out["checks"] = per_check
        out["dbs"] = [{"name": k, **v} for k, v in sorted(per_db.items())]
        worst = "ok"
        for pc in per_check.values():
            if pc["status"] == "error":
                worst = "error"
                break
            if pc["status"] == "diff":
                worst = "diff"
        out["status"] = worst
        age = time.time() - summary.stat().st_mtime
        out["meta"] = (f"last check {int(age // 60)}m ago"
                       if age < 86400 else
                       f"last check {int(age // 3600)}h ago")
    cl = rpt / "changelog.jsonl"
    if cl.exists():
        last = cl.read_text().splitlines()[-1:]
        if last:
            try:
                e = json.loads(last[0])
                out["meta"] += (f" | last write: {e.get('op')}"
                                f" {e.get('db', '')} at {e.get('at')}")
            except ValueError:
                pass
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes)
                         else body.encode())

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            return self._send(200, "text/html; charset=utf-8", PAGE)
        if self.path == "/api/data":
            hops = load_hops()
            data = {"now": time.strftime("%H:%M:%S"),
                    "hops": [_hop_data(n, h) for n, h in hops.items()]}
            return self._send(200, "application/json", json.dumps(data))
        if self.path.startswith("/report/"):
            name = self.path[len("/report/"):].split("/")[0].split("?")[0]
            f = REPORTS / name / "report.html"
            if f.exists() and Path(f).resolve().is_relative_to(
                    REPORTS.resolve()):
                return self._send(200, "text/html; charset=utf-8",
                                  f.read_bytes())
        return self._send(404, "text/plain", "not found")


def serve(port):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"migkit ui: http://127.0.0.1:{port} (ctrl-c to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
