# check_db_sanity.py
import os, sqlite3
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")
DB_PATH = DB_URL.replace("sqlite:///", "", 1)

def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # --- Counts ---
    cur.execute("SELECT COUNT(*) FROM reviews_raw")
    n_raw = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT review_id) FROM reviews_processed")
    n_proc = cur.fetchone()[0]

    n_missing = n_raw - n_proc

    print("📊 Database Sanity Report")
    print("-------------------------")
    print(f"Total raw reviews     : {n_raw}")
    print(f"Processed reviews     : {n_proc}")
    print(f"Missing (unprocessed) : {n_missing}")

    # --- Null check ---
    cur.execute("SELECT COUNT(*) FROM reviews_processed WHERE review_id IS NULL")
    n_nulls = cur.fetchone()[0]
    if n_nulls > 0:
        print(f"⚠️ Found {n_nulls} rows with NULL review_id in reviews_processed!")

    # --- Duplicate check ---
    cur.execute("""
        SELECT review_id, COUNT(*)
        FROM reviews_processed
        GROUP BY review_id
        HAVING COUNT(*) > 1
    """)
    dups = cur.fetchall()
    if dups:
        print(f"⚠️ Found {len(dups)} duplicate review_ids in reviews_processed:")
        for rid, cnt in dups[:10]:
            print(f"  - review_id={rid}, count={cnt}")
    else:
        print("✅ No duplicate review_ids found.")

    con.close()

if __name__ == "__main__":
    main()
