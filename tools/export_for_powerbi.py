# export_for_powerbi.py
import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")
DB_PATH = DB_URL.replace("sqlite:///", "", 1)

OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def main():
    con = sqlite3.connect(DB_PATH)

    # --- Reviews ---
    df_reviews = pd.read_sql("""
        SELECT id AS review_id, source, author, text, created_at
        FROM reviews_raw
    """, con)
    df_reviews.to_csv(f"{OUT_DIR}/reviews_clean.csv", index=False)
    print(f"→ {len(df_reviews)} reviews exported")

    # --- Processed ---
    df_proc = pd.read_sql("SELECT * FROM reviews_processed", con)

    # --- Aspects (use aspect_csv + confidence) ---
    aspects = df_proc[["review_id", "aspect_csv"]].copy()
    aspects["confidence"] = 0.7  # static confidence placeholder
    aspects.to_csv(f"{OUT_DIR}/aspects.csv", index=False)
    print(f"→ {len(aspects)} aspects exported")

    # --- Aspect-Sentiment (use aspect_csv + confidence) ---
    aspect_sent = df_proc[["review_id", "aspect_csv", "sentiment_label", "score_signed"]].copy()
    aspect_sent["confidence"] = 0.7
    aspect_sent.to_csv(f"{OUT_DIR}/aspect_sentiment.csv", index=False)
    print(f"→ {len(aspect_sent)} aspect-sentiment rows exported")

    # --- Daily metrics ---
    df_reviews["date"] = pd.to_datetime(df_reviews["created_at"]).dt.date
    joined = df_proc.merge(df_reviews[["review_id", "date"]], on="review_id", how="left")
    daily = (
        joined.groupby("date")
        .agg(avg_sentiment=("score_signed", "mean"), n_reviews=("review_id", "count"))
        .reset_index()
    )
    daily.to_csv(f"{OUT_DIR}/daily_metrics.csv", index=False)
    print(f"→ {len(daily)} daily metrics rows exported")

    # --- Topics ---
    if {"topic_id", "topic_label"}.issubset(df_proc.columns):
        topics = df_proc[["review_id", "topic_id", "topic_label"]].copy()
        topics["topic_prob"] = 1.0
        topics.to_csv(f"{OUT_DIR}/topics.csv", index=False)
        print(f"→ {len(topics)} topics exported")

    print(f"✅ Export complete → files in {OUT_DIR}/")
    con.close()

if __name__ == "__main__":
    main()