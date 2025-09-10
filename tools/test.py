import sqlite3

DB_PATH = "data/aspect_reviews.db"
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Ensure new columns exist
try:
    cur.execute("ALTER TABLE reviews_processed ADD COLUMN topic_source TEXT;")
except sqlite3.OperationalError:
    pass  # already exists

try:
    cur.execute("ALTER TABLE reviews_processed ADD COLUMN processed_at TIMESTAMP;")
except sqlite3.OperationalError:
    pass  # already exists

# Ensure aspects/aspect_csv are TEXT
# (SQLite is flexible, but we'll enforce TEXT)
cur.execute("PRAGMA table_info(reviews_processed);")

for row in cur.fetchall():
    print(row)  # check schema

con.commit()
con.close()