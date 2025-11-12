# migrate_duration.py
import sqlite3
conn = sqlite3.connect("queue.db")
conn.execute("ALTER TABLE jobs ADD COLUMN duration_seconds REAL;")
conn.commit()
conn.close()
print("Added duration_seconds.")
