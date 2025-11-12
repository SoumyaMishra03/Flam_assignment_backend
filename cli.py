# cli.py
import click
import time
from datetime import datetime, timedelta
from storage import Storage

@click.group()
def cli():
    """queuectl - A simple job queue CLI"""
    pass


# ---------------- Enqueue ----------------
@cli.command()
@click.option("--id", required=True, help="Job ID")
@click.option("--command", required=True, help="Command to run")
@click.option("--max-retries", default=None, type=int, help="Maximum retries (overrides default_max_retries config if set)")
@click.option("--timeout-seconds", default=None, type=int, help="Max runtime before job is killed (seconds)")
@click.option("--priority", default=0, type=int, help="Job priority (higher runs first)")
@click.option("--run-at", default=None, help="ISO timestamp (UTC) or +seconds delay")
def enqueue(id, command, max_retries, timeout_seconds, priority, run_at):
    """Add a new job to the queue"""
    db = Storage()
    now = datetime.utcnow().isoformat()

    # fall back to config default if not provided
    if max_retries is None:
        cfg_default = db.get_config("default_max_retries", default="3")
        try:
            max_retries = int(cfg_default)
        except Exception:
            max_retries = 3

    # Parse run_at: either ISO timestamp or +N seconds
    scheduled = None
    if run_at:
        try:
            if run_at.startswith("+"):
                delay = int(run_at[1:])
                scheduled = (datetime.utcnow() + timedelta(seconds=delay)).isoformat()
            else:
                scheduled = run_at  # assume ISO string
        except Exception as e:
            click.echo(f"❌ Invalid --run-at value: {run_at} ({e})")
            return

    try:
        db.conn.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, timeout_seconds, priority, run_at, created_at, updated_at)
            VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?)
        """, (id, command, max_retries, timeout_seconds, priority, scheduled, now, now))
        db.conn.commit()
        pr = f"priority={priority}"
        ra = f", run_at={scheduled}" if scheduled else ""
        click.echo(f"✅ Job {id} enqueued ({pr}{ra}).")
    except Exception as e:
        click.echo(f"❌ Failed to enqueue job: {e}")


# ---------------- List Jobs ----------------
@cli.command(name="list")
@click.option("--state", default=None, help="Filter jobs by state (pending, processing, completed, failed, dead)")
def list_jobs(state):
    """List jobs in the queue"""
    db = Storage()
    cur = db.conn.cursor()
    base_cols = "id, command, state, attempts, max_retries, priority, run_at, duration_seconds"
    if state:
        cur.execute(f"SELECT {base_cols} FROM jobs WHERE state=? ORDER BY created_at", (state,))
    else:
        cur.execute(f"SELECT {base_cols} FROM jobs ORDER BY created_at")
    rows = cur.fetchall()

    if not rows:
        click.echo("No jobs found.")
        return

    for row in rows:
        run_at = row["run_at"] if row["run_at"] else "-"
        dur = f"{row['duration_seconds']:.3f}s" if row["duration_seconds"] is not None else "-"
        click.echo(f"{row['id']} | {row['command']} | state={row['state']} | attempts={row['attempts']}/{row['max_retries']} | priority={row['priority']} | run_at={run_at} | duration={dur}")


# ---------------- Status ----------------
@cli.command()
def status():
    """Show summary of job states"""
    db = Storage()
    cur = db.conn.cursor()
    cur.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
    rows = cur.fetchall()

    if not rows:
        click.echo("No jobs in the system yet.")
        return

    click.echo("📊 Job Status Summary:")
    for row in rows:
        click.echo(f"  {row['state']}: {row['count']}")


# ---------------- Metrics ----------------
@cli.command()
def metrics():
    """Show job metrics summary"""
    db = Storage()
    cur = db.conn.cursor()

    # Counts
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='completed'")
    completed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='failed'")
    failed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='dead'")
    dead = cur.fetchone()["c"]

    # Average duration for completed jobs
    cur.execute("SELECT AVG(duration_seconds) AS avg_dur FROM jobs WHERE state='completed' AND duration_seconds IS NOT NULL")
    avg_duration = cur.fetchone()["avg_dur"]

    click.echo("📈 Metrics Summary")
    click.echo(f"  Completed jobs: {completed}")
    click.echo(f"  Failed jobs: {failed}")
    click.echo(f"  Dead jobs: {dead}")
    click.echo(f"  Avg duration (s): {avg_duration:.3f}" if avg_duration is not None else "  Avg duration: N/A")


# ---------------- Worker ----------------
@cli.command()
@click.option("--count", default=1, help="Number of workers to start")
@click.option("--lease-seconds", default=None, help="Lease duration to prevent double-claims (uses config if set)")
@click.option("--backoff-base", default=None, help="Exponential backoff base for retries (uses config if set)")
@click.option("--poll-interval", default=None, help="Idle polling interval (seconds) (uses config if set)")
def worker(count, lease_seconds, backoff_base, poll_interval):
    """Start background workers with leases and graceful shutdown"""
    import threading
    from worker import Worker

    db = Storage()

    # Load config defaults if args are not provided
    if lease_seconds is None:
        lease_seconds = int(db.get_config("lease_seconds", default="30"))
    if backoff_base is None:
        backoff_base = int(db.get_config("backoff_base", default="2"))
    if poll_interval is None:
        poll_interval = float(db.get_config("poll_interval", default="1.0"))

    stop_event = threading.Event()
    workers = []

    for i in range(count):
        w = Worker(worker_id=f"worker-{i+1}",
                   lease_seconds=lease_seconds,
                   backoff_base=backoff_base,
                   poll_interval=poll_interval,
                   stop_event=stop_event)
        t = threading.Thread(target=w.run, name=f"worker-thread-{i+1}", daemon=True)
        workers.append((w, t))
        click.echo(f"🚀 Starting {w.worker_id} (lease={lease_seconds}s, backoff_base={backoff_base}, poll={poll_interval}s)")
        t.start()

    click.echo("Press Ctrl+C to stop workers gracefully.")

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        click.echo("\n🛑 Stopping workers ...")
        stop_event.set()
        for _, t in workers:
            t.join(timeout=5.0)
        click.echo("✅ Workers stopped cleanly.")


# ---------------- Dead Letter Queue ----------------
@cli.group()
def dlq():
    """Dead Letter Queue operations"""
    pass

@dlq.command("list")
def dlq_list():
    """List jobs in DLQ"""
    db = Storage()
    cur = db.conn.cursor()
    cur.execute("SELECT id, command, attempts, error, priority, run_at, duration_seconds FROM jobs WHERE state='dead' ORDER BY created_at")
    rows = cur.fetchall()
    if not rows:
        click.echo("No jobs in DLQ.")
        return
    for row in rows:
        run_at = row["run_at"] if row["run_at"] else "-"
        dur = f"{row['duration_seconds']:.3f}s" if row["duration_seconds"] is not None else "-"
        click.echo(f"{row['id']} | {row['command']} | attempts={row['attempts']} | priority={row['priority']} | run_at={run_at} | duration={dur} | error={row['error']}")

@dlq.command("retry")
@click.argument("job_id")
def dlq_retry(job_id):
    """Retry a DLQ job by resetting it to pending"""
    db = Storage()
    now = datetime.utcnow().isoformat()
    db.conn.execute("""
        UPDATE jobs
        SET state='pending', attempts=0, updated_at=?, error=NULL, next_run_at=NULL, worker_id=NULL, lease_until=NULL
        WHERE id=? AND state='dead'
    """, (now, job_id))
    db.conn.commit()
    click.echo(f"♻️ Job {job_id} moved back to pending.")


# ---------------- Config management ----------------
@cli.group()
def config():
    """Runtime configuration for workers and defaults"""
    pass

@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    """Set a config key to a value"""
    db = Storage()
    now = datetime.utcnow().isoformat()
    db.conn.execute("""
        INSERT INTO config (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, value, now))
    db.conn.commit()
    click.echo(f"🛠️ Config '{key}' set to '{value}'.")

@config.command("get")
@click.argument("key")
@click.option("--default", default=None, help="Fallback if key not set")
def config_get(key, default):
    """Get a config key"""
    db = Storage()
    cur = db.conn.cursor()
    cur.execute("SELECT value, updated_at FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    if not row:
        if default is not None:
            click.echo(f"{key}={default} (default)")
        else:
            click.echo(f"{key} not set")
        return
    click.echo(f"{key}={row['value']} (updated_at={row['updated_at']})")

@config.command("list")
def config_list():
    """List all config keys"""
    db = Storage()
    cur = db.conn.cursor()
    cur.execute("SELECT key, value, updated_at FROM config ORDER BY key")
    rows = cur.fetchall()
    if not rows:
        click.echo("No config keys set.")
        return
    for row in rows:
        click.echo(f"{row['key']}={row['value']} (updated_at={row['updated_at']})")


# ---------------- Rescue operations ----------------
@cli.group()
def rescue():
    """Recovery tools for stuck jobs"""
    pass

@rescue.command("leases")
@click.option("--older-than-seconds", default=60, help="Clear leases older than N seconds")
def rescue_leases(older_than_seconds):
    """Clear expired leases and return jobs to pending"""
    db = Storage()
    cutoff = (datetime.utcnow().timestamp() - older_than_seconds)
    cutoff_iso = datetime.utcfromtimestamp(cutoff).isoformat()

    cur = db.conn.cursor()
    cur.execute("""
        SELECT id FROM jobs
        WHERE state='processing' AND lease_until IS NOT NULL AND lease_until <= ?
    """, (cutoff_iso,))
    rows = cur.fetchall()

    if not rows:
        click.echo("No expired leases found.")
        return

    ids = [r["id"] for r in rows]
    now = datetime.utcnow().isoformat()
    db.conn.execute(f"""
        UPDATE jobs
        SET state='pending', worker_id=NULL, lease_until=NULL, updated_at=?
        WHERE id IN ({",".join("?" for _ in ids)})
    """, (now, *ids))
    db.conn.commit()
    click.echo(f"🔧 Cleared leases and returned {len(ids)} job(s) to pending: {', '.join(ids)}")

@cli.command()
@click.argument("job_id")
def show(job_id):
    """Show details of a single job"""
    db = Storage()
    cur = db.conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row:
        click.echo(f"❌ Job {job_id} not found.")
        return

    click.echo(f"🔎 Job {row['id']}")
    click.echo(f"  Command: {row['command']}")
    click.echo(f"  State: {row['state']}")
    click.echo(f"  Attempts: {row['attempts']}/{row['max_retries']}")
    click.echo(f"  Priority: {row['priority']}")
    click.echo(f"  Run at: {row['run_at'] or '-'}")
    click.echo(f"  Created: {row['created_at']}")
    click.echo(f"  Started: {row['started_at'] or '-'}")
    click.echo(f"  Finished: {row['finished_at'] or '-'}")
    click.echo(f"  Duration: {row['duration_seconds']:.3f}s" if row['duration_seconds'] else "  Duration: -")
    click.echo(f"  Exit code: {row['exit_code'] if row['exit_code'] is not None else '-'}")
    click.echo(f"  Error: {row['error'] or '-'}")
    click.echo("  Output:")
    click.echo(row['output'] or "(no output)")

# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    cli()
