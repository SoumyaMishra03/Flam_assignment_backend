# dashboard.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from storage import Storage

app = FastAPI()
db = Storage()

# ---------- Shared UI ----------
BASE_STYLE = """
  body { font-family: Arial, sans-serif; margin: 0; background: #f9f9f9; color: #333; }
  h1 { background: #2196F3; color: white; padding: 15px; margin: 0; }
  h2 { margin-top: 30px; color: #2196F3; }
  .container { padding: 20px; }
  .navbar { background: #1976D2; padding: 10px 20px; display: flex; gap: 20px; }
  .navbar a { color: white; text-decoration: none; font-weight: bold; }
  .navbar a:hover { text-decoration: underline; }
  table { border-collapse: collapse; width: 100%; margin-top: 10px; background: white; }
  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
  th { background-color: #2196F3; color: white; }
  tr:nth-child(even) { background-color: #f2f2f2; }
  tr:hover { background-color: #e0f7fa; }
  canvas { margin-top: 20px; display: block; max-width: 800px; }
  a { color: #1976D2; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap: 16px; margin-top: 20px; }
  .card { background: white; border: 1px solid #ddd; border-radius: 6px; padding: 12px; }
  .muted { color: #555; }
"""

def page(title: str, body_html: str, include_chart_js: bool = False) -> str:
    script_tag = '<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>' if include_chart_js else ''
    return f"""
    <html>
    <head>
      <title>{title}</title>
      {script_tag}
      <style>{BASE_STYLE}</style>
    </head>
    <body>
      <h1>{title}</h1>
      <div class="navbar">
        <a href="/">🏠 Home</a>
        <a href="/metrics">📈 Metrics</a>
        <a href="/dlq">🗑 DLQ</a>
        <a href="/config">⚙ Config</a>
      </div>
      <div class="container">
        {body_html}
      </div>
    </body>
    </html>
    """

# ---------- Home ----------
@app.get("/", response_class=HTMLResponse)
def home():
    cur = db.conn.cursor()
    cur.execute("""
        SELECT id, command, state, attempts, max_retries, priority, run_at, duration_seconds, created_at
        FROM jobs ORDER BY created_at DESC LIMIT 50
    """)
    rows = cur.fetchall()

    table_html = """
    <h2>Recent jobs</h2>
    <table>
      <tr><th>ID</th><th>Command</th><th>State</th><th>Attempts</th><th>Priority</th><th>Run at</th><th>Duration</th></tr>
    """
    for r in rows:
        dur = f"{r['duration_seconds']:.3f}s" if r['duration_seconds'] else "-"
        table_html += f"<tr><td><a href='/job/{r['id']}'>{r['id']}</a></td><td>{r['command']}</td><td>{r['state']}</td><td>{r['attempts']}/{r['max_retries']}</td><td>{r['priority']}</td><td>{r['run_at'] or '-'}</td><td>{dur}</td></tr>"
    table_html += "</table>"

    charts_html = """
      <h2>Job states</h2>
      <canvas id="jobChart"></canvas>

      <h2>Average duration trend</h2>
      <canvas id="durationChart"></canvas>

      <script>
        async function loadCharts() {
          const resStates = await fetch('/metrics/json');
          const dataStates = await resStates.json();

          new Chart(document.getElementById('jobChart'), {
            type: 'pie',
            data: {
              labels: ['Completed', 'Failed', 'Dead'],
              datasets: [{
                data: [dataStates.completed, dataStates.failed, dataStates.dead],
                backgroundColor: ['#4CAF50', '#FF9800', '#F44336']
              }]
            }
          });

          const resDur = await fetch('/metrics/durations');
          const dataDur = await resDur.json();

          new Chart(document.getElementById('durationChart'), {
            type: 'line',
            data: {
              labels: dataDur.timestamps,
              datasets: [{
                label: 'Avg Duration (s)',
                data: dataDur.durations,
                borderColor: '#2196F3',
                backgroundColor: '#BBDEFB',
                fill: true
              }]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              scales: { y: { beginAtZero: true } }
            }
          });
        }
        loadCharts();
      </script>
    """

    return page("📊 Queue Dashboard", table_html + charts_html, include_chart_js=True)

# ---------- Metrics (page) ----------
@app.get("/metrics", response_class=HTMLResponse)
def metrics_page():
    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='completed'")
    completed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='failed'")
    failed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='dead'")
    dead = cur.fetchone()["c"]
    cur.execute("SELECT AVG(duration_seconds) AS avg_dur FROM jobs WHERE state='completed' AND duration_seconds IS NOT NULL")
    avg_duration = cur.fetchone()["avg_dur"]

    cards = f"""
      <div class="cards">
        <div class="card"><h3>Completed</h3><p>{completed}</p></div>
        <div class="card"><h3>Failed</h3><p>{failed}</p></div>
        <div class="card"><h3>Dead</h3><p>{dead}</p></div>
        <div class="card"><h3>Avg duration</h3><p>{(f"{avg_duration:.3f}s" if avg_duration else "N/A")}</p></div>
      </div>
      <p class="muted">Tip: Use the CLI "metrics" command for scriptable outputs.</p>
    """
    return page("📈 Metrics", cards)

# ---------- Metrics (JSON APIs for charts) ----------
@app.get("/metrics/json", response_class=JSONResponse)
def metrics_json():
    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='completed'")
    completed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='failed'")
    failed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='dead'")
    dead = cur.fetchone()["c"]
    cur.execute("SELECT AVG(duration_seconds) AS avg_dur FROM jobs WHERE state='completed' AND duration_seconds IS NOT NULL")
    avg_duration = cur.fetchone()["avg_dur"]
    return {"completed": completed, "failed": failed, "dead": dead, "avg_duration": avg_duration}

@app.get("/metrics/durations", response_class=JSONResponse)
def metrics_durations():
    cur = db.conn.cursor()
    cur.execute("""
        SELECT substr(created_at, 1, 10) AS day, AVG(duration_seconds) AS avg_dur
        FROM jobs
        WHERE state='completed' AND duration_seconds IS NOT NULL
        GROUP BY day ORDER BY day
    """)
    rows = cur.fetchall()
    timestamps = [r["day"] for r in rows]
    durations = [round(r["avg_dur"], 3) for r in rows]
    return {"timestamps": timestamps, "durations": durations}

# ---------- DLQ ----------
@app.get("/dlq", response_class=HTMLResponse)
def dlq_page():
    cur = db.conn.cursor()
    cur.execute("""
        SELECT id, command, attempts, error, priority, run_at, duration_seconds, created_at
        FROM jobs WHERE state='dead' ORDER BY created_at DESC
    """)
    rows = cur.fetchall()

    body = """
      <h2>Dead letter queue</h2>
      <table>
        <tr><th>ID</th><th>Command</th><th>Attempts</th><th>Error</th><th>Priority</th><th>Run at</th><th>Duration</th></tr>
    """
    if not rows:
        body += "</table><p class='muted'>No jobs in DLQ.</p>"
    else:
        for r in rows:
            dur = f"{r['duration_seconds']:.3f}s" if r['duration_seconds'] else "-"
            err = r['error'] or "-"
            body += f"<tr><td><a href='/job/{r['id']}'>{r['id']}</a></td><td>{r['command']}</td><td>{r['attempts']}</td><td>{err}</td><td>{r['priority']}</td><td>{r['run_at'] or '-'}</td><td>{dur}</td></tr>"
        body += "</table><p class='muted'>Use CLI dlq commands to inspect or retry.</p>"

    return page("🗑 Dead Letter Queue", body)

# ---------- Config ----------
@app.get("/config", response_class=HTMLResponse)
def config_page():
    cur = db.conn.cursor()
    cur.execute("SELECT key, value, updated_at FROM config ORDER BY key")
    rows = cur.fetchall()

    body = """
      <h2>Runtime configuration</h2>
      <table>
        <tr><th>Key</th><th>Value</th><th>Updated</th></tr>
    """
    if not rows:
        body += "</table><p class='muted'>No config entries found.</p>"
    else:
        for r in rows:
            body += f"<tr><td>{r['key']}</td><td>{r['value']}</td><td>{r['updated_at']}</td></tr>"
        body += "</table><p class='muted'>Use CLI config set/get to manage values.</p>"

    return page("⚙ Config", body)

# ---------- Job detail ----------
@app.get("/job/{job_id}", response_class=HTMLResponse)
def job_detail(job_id: str):
    cur = db.conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row:
        return page("❌ Job not found", f"<p>Job {job_id} not found.</p>")

    duration = f"{row['duration_seconds']:.3f}s" if row['duration_seconds'] else "-"
    exit_code = row['exit_code'] if row['exit_code'] is not None else "-"
    error = row['error'] or "-"

    body = f"""
      <h2>Job {row['id']}</h2>
      <div class="cards">
        <div class="card"><b>State</b><p>{row['state']}</p></div>
        <div class="card"><b>Attempts</b><p>{row['attempts']}/{row['max_retries']}</p></div>
        <div class="card"><b>Priority</b><p>{row['priority']}</p></div>
        <div class="card"><b>Duration</b><p>{duration}</p></div>
        <div class="card"><b>Exit code</b><p>{exit_code}</p></div>
      </div>

      <h3>Command</h3>
      <p class="muted">{row['command']}</p>

      <h3>Timestamps</h3>
      <table>
        <tr><th>Created</th><td>{row['created_at']}</td></tr>
        <tr><th>Run at</th><td>{row['run_at'] or '-'}</td></tr>
        <tr><th>Started</th><td>{row['started_at'] or '-'}</td></tr>
        <tr><th>Finished</th><td>{row['finished_at'] or '-'}</td></tr>
        <tr><th>Updated</th><td>{row['updated_at'] or '-'}</td></tr>
      </table>

      <h3>Error</h3>
      <pre>{error}</pre>

      <h3>Output</h3>
      <pre>{row['output'] or "(no output)"}</pre>

      <p><a href="/job/{row['id']}/download">⬇ Download output log</a></p>
    """
    return page(f"🔎 Job {row['id']} Detail", body)

# ---------- Download output ----------
@app.get("/job/{job_id}/download", response_class=PlainTextResponse)
def download_output(job_id: str):
    cur = db.conn.cursor()
    cur.execute("SELECT output FROM jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row or not row["output"]:
        return PlainTextResponse("(no output)", media_type="text/plain")
    return PlainTextResponse(row["output"], media_type="text/plain")
