import sqlite3

conn = sqlite3.connect("queue.db")
conn.execute("ALTER TABLE jobs ADD COLUMN timeout_seconds INTEGER;")
conn.execute("ALTER TABLE jobs ADD COLUMN started_at TEXT;")
conn.execute("ALTER TABLE jobs ADD COLUMN finished_at TEXT;")
conn.execute("ALTER TABLE jobs ADD COLUMN output TEXT;")
conn.commit()
conn.close()
