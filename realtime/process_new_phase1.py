
import os, pandas as pd, numpy as np
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy import text as sqltext
from sqlalchemy.orm import Session
from src.db_models import engine, SessionLocal, Review, Processed, init_db
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline


# -------------------------------------------
#  Aspect vocabulary (simple keyword search)
# -------------------------------------------
VOCAB_ASPECTS = [
    "battery","camera","screen","ui","support",
    "shipping","packaging","build","delivery","quality","price"
]

def simple_aspects(text):
    """Return list of aspects found in text (keyword-based)."""
    t = (text or "").lower()
    return [w for w in VOCAB_ASPECTS if w in t]

# -------------------------------------------
#  Weakly supervised sentiment baseline
# -------------------------------------------
def train_baseline(df):
    pos_words = ["great","excellent","superb","helpful","quick","premium","bright","snappy"]
    neg_words = ["drains","overheats","grainy","crashes","delayed","damaged","lags","inconsistent"]

    def weak_label(t):
        t = (t or "").lower()
        pos = any(w in t for w in pos_words)
        neg = any(w in t for w in neg_words)
        if pos and not neg: return "POS"
        if neg and not pos: return "NEG"
        return "NEU"

    df["weak_label"] = df["text"].apply(weak_label)
    train = df[df["weak_label"] != "NEU"]

    if len(train) < 20:
        return None

    pipe = Pipeline([
        ("tfidf", TfidfVectorizer(min_df=2, ngram_range=(1,2))),
        ("logreg", LogisticRegression(max_iter=1000))
    ])
    pipe.fit(train["text"], train["weak_label"])
    return pipe

# -------------------------------------------
#  Main pipeline
# -------------------------------------------
def main():
    init_db()
    sess = SessionLocal()
    try:
        rows = sess.query(Review).outerjoin(
            Processed, Processed.review_id == Review.id
        ).filter(Processed.id == None).limit(1000).all()

        if not rows:
            print("No new reviews found.")
            return

        df = pd.DataFrame([{"id": r.id, "text": r.text or ""} for r in rows])

        # Train baseline classifier
        model = train_baseline(df.copy())
        if model is None:
            preds = [("NEU", 0.0)] * len(df)
        else:
            proba = model.predict_proba(df["text"])
            labels = model.classes_[np.argmax(proba, axis=1)]
            scores = np.max(proba, axis=1)
            preds = list(zip(labels, scores))

        # Save processed rows
        for (rid, text), (lab, score) in zip(df[["id","text"]].values, preds):
            aspects = simple_aspects(text)
            sign = {"POS":1.0, "NEG":-1.0, "NEU":0.0}[lab]
            rec = Processed(
                review_id=int(rid),
                sentiment_label=lab,
                score=float(score),
                score_signed=float(score*sign),
                aspect_csv=",".join(aspects)
            )
            sess.add(rec)
        sess.commit()
        print(f"Processed {len(df)} reviews.")
        export_power_bi_tables(sess)
    finally:
        sess.close()

# -------------------------------------------
#  Power BI export
# -------------------------------------------
def export_power_bi_tables(sess: Session):
    df_reviews = pd.read_sql(
        sqltext("SELECT id as review_id, source, author, text, created_at FROM reviews_raw"),
        con=engine
    )
    df_reviews.to_csv("data/processed/reviews_clean.csv", index=False)

    rows = []
    proc = pd.read_sql(
        sqltext("SELECT review_id, aspect_csv, sentiment_label, score_signed FROM reviews_processed"),
        con=engine
    )
    for _, r in proc.iterrows():
        for a in (r["aspect_csv"].split(",") if r["aspect_csv"] else []):
            if a:
                rows.append({"review_id": r["review_id"], "aspect": a, "confidence": 0.7})
    pd.DataFrame(rows).to_csv("data/processed/aspects.csv", index=False)

    aspect_sent = []
    for _, r in proc.iterrows():
        aspects = r["aspect_csv"].split(",") if r["aspect_csv"] else []
        for a in aspects:
            if a:
                aspect_sent.append({
                    "review_id": r["review_id"],
                    "aspect": a,
                    "sentiment_label": r["sentiment_label"],
                    "score_signed": r["score_signed"]
                })
    pd.DataFrame(aspect_sent).to_csv("data/processed/aspect_sentiment.csv", index=False)

    df_reviews["date"] = pd.to_datetime(df_reviews["created_at"]).dt.date
    joined = proc.merge(df_reviews[["review_id","date"]], on="review_id", how="left")
    daily = joined.groupby("date").agg(
        avg_sentiment=("score_signed","mean"),
        n_reviews=("review_id","count")
    ).reset_index()
    daily.to_csv("data/processed/daily_metrics.csv", index=False)

    pd.DataFrame(columns=["review_id","topic_id","topic_label","topic_prob"]).to_csv(
        "data/processed/topics.csv", index=False
    )
    print("Exported Power BI tables to data/processed/.")

# -------------------------------------------
if __name__ == "__main__":
    main()