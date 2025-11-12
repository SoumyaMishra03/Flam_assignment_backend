import sqlite3

conn = sqlite3.connect("queue.db")
conn.execute("ALTER TABLE jobs ADD COLUMN run_at TEXT;")
conn.commit()
conn.close()
print("Migration complete.")
