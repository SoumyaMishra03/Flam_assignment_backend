import sqlite3

conn = sqlite3.connect("queue.db")
conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0;")
conn.commit()
conn.close()
