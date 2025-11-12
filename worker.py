# worker.py
import subprocess
import time
import uuid
from datetime import datetime, timedelta
from storage import Storage

class Worker:
    def __init__(self, worker_id=None, lease_seconds=30, backoff_base=2, poll_interval=1.0, stop_event=None):
        self.db = Storage()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = lease_seconds
        self.backoff_base = backoff_base
        self.poll_interval = poll_interval
        self.stop_event = stop_event  # threading.Event() passed in by CLI

    def run(self):
        while not (self.stop_event and self.stop_event.is_set()):
            job = self._claim_one_ready_job()
            if not job:
                time.sleep(self.poll_interval)
                continue
            self._process_job(job)

    def _now(self):
        return datetime.utcnow()

    def _log_transition(self, job_id, old_state, new_state, extra=""):
        now = self._now().isoformat()
        print(f"[{now}] Job {job_id}: {old_state} → {new_state} {extra}")

    def _claim_one_ready_job(self):
        """
        Atomically claim one job that is ready:
        - state = 'pending' OR (state='failed' AND next_run_at <= now)
        - run_at is NULL or <= now (scheduled jobs)
        - lease is absent or expired
        Preference: highest priority first, then oldest.
        """
        now = self._now()
        lease_until = (now + timedelta(seconds=self.lease_seconds)).isoformat()
        cur = self.db.conn.cursor()

        self.db.conn.execute("BEGIN IMMEDIATE")

        cur.execute("""
            SELECT id, state, started_at FROM jobs
            WHERE (
                state='pending'
                OR (state='failed' AND (next_run_at IS NOT NULL AND next_run_at <= ?))
            )
            AND (run_at IS NULL OR run_at <= ?)
            AND (lease_until IS NULL OR lease_until <= ?)
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
        """, (now.isoformat(), now.isoformat(), now.isoformat()))
        row = cur.fetchone()
        if not row:
            self.db.conn.execute("COMMIT")
            return None

        job_id = row["id"]
        old_state = row["state"]
        now_iso = now.isoformat()

        updated = self.db.conn.execute("""
            UPDATE jobs
            SET state='processing', worker_id=?, lease_until=?, started_at=COALESCE(started_at, ?), updated_at=?
            WHERE id=? AND (lease_until IS NULL OR lease_until <= ?)
        """, (self.worker_id, lease_until, now_iso, now_iso, job_id, now_iso)).rowcount
        self.db.conn.execute("COMMIT")

        if updated != 1:
            return None  # lost the race to another worker

        cur = self.db.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        self._log_transition(job_id, old_state, "processing", f"(claimed by {self.worker_id})")
        return job

    def _process_job(self, job):
        start_time = self._now()
        try:
            timeout = job["timeout_seconds"]
            result = subprocess.run(
                job["command"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout if timeout else None  # Enforce timeout if provided
            )
            exit_code = result.returncode
            output = (result.stdout or "") + (result.stderr or "")
            now_iso = self._now().isoformat()
            duration = (self._now() - start_time).total_seconds()

            if exit_code == 0:
                self.db.conn.execute("""
                    UPDATE jobs
                    SET state='completed', exit_code=?, error=NULL, output=?, lease_until=NULL,
                        finished_at=?, updated_at=?, duration_seconds=?
                    WHERE id=?
                """, (exit_code, output, now_iso, now_iso, duration, job["id"]))
                self.db.conn.commit()
                self._log_transition(job["id"], "processing", "completed", f"(exit_code={exit_code}, duration={duration:.3f}s)")
            else:
                self._handle_failure(job, exit_code, now_iso, duration, error=result.stderr)
        except subprocess.TimeoutExpired:
            now_iso = self._now().isoformat()
            duration = (self._now() - start_time).total_seconds()
            self._handle_failure(job, -1, now_iso, duration, error="timeout")

    def _handle_failure(self, job, exit_code, now_iso, duration, error=None):
        attempts = (job["attempts"] or 0) + 1
        if attempts >= job["max_retries"]:
            self.db.conn.execute("""
                UPDATE jobs
                SET state='dead', attempts=?, exit_code=?, error=?, lease_until=NULL, finished_at=?, updated_at=?, duration_seconds=?
                WHERE id=?
            """, (attempts, exit_code, error, now_iso, now_iso, duration, job["id"]))
            self.db.conn.commit()
            self._log_transition(job["id"], "processing", "dead", f"(attempts={attempts}, exit_code={exit_code}, duration={duration:.3f}s, error={error})")
        else:
            delay = self.backoff_base ** attempts
            next_run_at = (self._now() + timedelta(seconds=delay)).isoformat()
            self.db.conn.execute("""
                UPDATE jobs
                SET state='failed', attempts=?, exit_code=?, error=?, next_run_at=?, lease_until=NULL, updated_at=?, duration_seconds=?
                WHERE id=?
            """, (attempts, exit_code, error, next_run_at, now_iso, duration, job["id"]))
            self.db.conn.commit()
            self._log_transition(job["id"], "processing", "failed",
                                 f"(attempts={attempts}, exit_code={exit_code}, retry_in={delay}s, duration={duration:.3f}s, error={error})")
