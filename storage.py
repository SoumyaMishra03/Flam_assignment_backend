# storage.py
import sqlite3
from datetime import datetime

class Storage:
    def __init__(self, db_path="queue.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Better concurrency for multiple workers
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")

        self._init_schema()

    def _init_schema(self):
        # Jobs table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            exit_code INTEGER,
            error TEXT,
            worker_id TEXT,
            lease_until TEXT,
            next_run_at TEXT,
            timeout_seconds INTEGER,
            priority INTEGER DEFAULT 0,
            run_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            output TEXT,
            duration_seconds REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        # Config table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        self.conn.commit()

    # ---------------- Config helpers ----------------
    def get_config(self, key, default=None):
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM config WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

    def set_config(self, key, value):
        now = datetime.utcnow().isoformat()
        self.conn.execute("""
            INSERT INTO config (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """, (key, str(value), now))
        self.conn.commit()
