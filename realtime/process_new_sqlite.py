# realtime/process_new.py

import os, re, sqlite3, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
from nlp.aspects import AspectTagger

print("process_new.py STARTED")

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")
assert DB_URL.startswith("sqlite:///"), f"Only sqlite is supported here. Got: {DB_URL}"
DB_PATH = DB_URL.replace("sqlite:///", "", 1)

ASPECTS_TOP_K   = int(os.getenv("ASPECTS_TOP_K", "5"))
ASPECTS_MIN_HITS= int(os.getenv("ASPECTS_MIN_HITS", "1"))
tagger = AspectTagger(top_k=ASPECTS_TOP_K, min_hits=ASPECTS_MIN_HITS)

def tag_aspects(txt: str) -> str:
    labels = tagger.tag(txt or "").labels
    return ",".join(labels)

def ensure_schema(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews_processed (
            id INTEGER PRIMARY KEY,
            review_id INTEGER NOT NULL,
            aspects TEXT,
            aspect_csv TEXT,
            sentiment_label TEXT,
            sentiment_score REAL,
            processed_at TIMESTAMP
        )
    """)
    cur.execute("""
        DELETE FROM reviews_processed
        WHERE review_id IS NOT NULL
          AND id NOT IN (
            SELECT MAX(id) FROM reviews_processed
            WHERE review_id IS NOT NULL
            GROUP BY review_id
          )
    """)
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_reviews_processed_review_id
                   ON reviews_processed(review_id)""")
    cur.execute("""UPDATE reviews_processed
                   SET processed_at = CURRENT_TIMESTAMP
                   WHERE processed_at IS NULL""")

def fetch_new(cur, limit=500):
    cur.execute("""
        SELECT r.id, r.text
        FROM reviews_raw r
        LEFT JOIN reviews_processed p ON p.review_id = r.id
        WHERE p.review_id IS NULL
        ORDER BY r.id ASC
        LIMIT ?
    """, (limit,))
    return cur.fetchall()

def upsert(cur, rows):
    if not rows:
        return 0
    payload = []
    for rid, txt in rows:
        aspects_csv = tag_aspects(txt)
        payload.append((rid, aspects_csv, aspects_csv))
    cur.executemany("""
        INSERT INTO reviews_processed (review_id, aspects, aspect_csv, processed_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(review_id) DO UPDATE SET
          aspects      = COALESCE(excluded.aspects, reviews_processed.aspects),
          aspect_csv   = COALESCE(excluded.aspect_csv, reviews_processed.aspect_csv),
          processed_at = CURRENT_TIMESTAMP
    """, payload)
    return len(rows)

def main():
    print("DB_PATH:", os.path.abspath(DB_PATH))
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        ensure_schema(cur); con.commit()
        cur.execute("""
            SELECT COUNT(*)
            FROM reviews_raw r
            LEFT JOIN reviews_processed p ON p.review_id = r.id
            WHERE p.review_id IS NULL
        """)
        need_new = cur.fetchone()[0]
        print("new raw rows to process:", need_new)
        total = 0
        while True:
            batch = fetch_new(cur, limit=500)
            if not batch: break
            n = upsert(cur, batch); con.commit()
            total += n
            print(f"Inserted/updated {n} rows (cumulative: {total})")
        print("No new rows. Done." if total==0 else f"Done. Inserted/updated total: {total}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
