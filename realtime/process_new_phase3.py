# realtime/process_new_phase3.py
import os, sqlite3, sys
import json
import pandas as pd
from transformers import pipeline
from bertopic import BERTopic
from keybert import KeyBERT
import spacy
from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer
from hdbscan import HDBSCAN
from dotenv import load_dotenv

sys.path.append("/opt/airflow/src")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

print("process_new_phase3.py STARTED")

# --- Load environment ---
load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")
DB_PATH = DB_URL.replace("sqlite:///", "", 1)

# --- Sentiment pipeline (3-class) ---
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest"
)

# --- Aspect extractor (KeyBERT + spaCy) ---
kw_model = KeyBERT()
nlp = spacy.load("en_core_web_sm")

def extract_aspects(txt: str):
    if not txt:
        return ""
    doc = nlp(txt)
    noun_chunks = [chunk.text for chunk in doc.noun_chunks if len(chunk.text) > 2]

    keywords = kw_model.extract_keywords(
        txt, keyphrase_ngram_range=(1, 2), stop_words="english", top_n=5
    )
    keys = [kw for kw, _ in keywords]

    combined = list(set(keys + noun_chunks))
    return ",".join(combined[:5])

def analyze_sentiment(txt: str):
    res = sentiment_pipeline(txt[:512])[0]  # clip text
    label = res["label"].upper()
    score = float(res["score"])
    sign = {"POSITIVE": 1, "NEGATIVE": -1, "NEUTRAL": 0}.get(label, 0)
    return label, score, score * sign

# --- Load or train BERTopic ---
MODEL_PATH = "/opt/airflow/models/bertopic_model"
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)

vectorizer = CountVectorizer(
    stop_words="english",
    min_df=2,
    max_df=0.8,
    ngram_range=(1, 2)
)

hdbscan_model = HDBSCAN(
    min_cluster_size=3,
    min_samples=1,
    gen_min_span_tree=True,
    prediction_data=True   # needed for transform()
)

if os.path.exists(MODEL_PATH):
    print(f"Loading BERTopic model from {MODEL_PATH}")
    topic_model = BERTopic.load(MODEL_PATH)
else:
    print("Training new BERTopic model (first run)...")
    topic_model = BERTopic(
        vectorizer_model=vectorizer,
        hdbscan_model=hdbscan_model,
        nr_topics=None
    )
    if os.path.exists(DB_PATH):
        con = sqlite3.connect(DB_PATH)
        all_reviews = pd.read_sql("SELECT text FROM reviews_raw", con)
        con.close()
        if not all_reviews.empty:
            docs = all_reviews["text"].tolist()
            topic_model.fit(docs)
            topic_model.reduce_topics(docs, nr_topics=10)  # force more diversity
            topic_model.save(MODEL_PATH)
            print(f"Trained and saved BERTopic model with {len(all_reviews)} docs.")
    else:
        print(" No DB found, starting with empty BERTopic model.")

# --- Junk words to filter out ---
JUNK_WORDS = {
    # basic stopwords
    "the","a","an","is","are","was","were","be","been","do","did","does",
    "at","in","on","with","to","from","of","by","about","into","over","under",
    "so","very","much","many","few","more","most","some","any","all","every",
    "can","could","should","would","will","may","might","must",

    # casual / reddit filler
    "lol","haha","omg","damn","bro","dude","hey","hi","hello",
    "pls","please","thanks","thank","thx","yep","yeah","nope","ok","okay",
    "idk","imo","imho","btw","wtf","smh","lmao","rofl",

    # tech filler
    "app","apps","update","version","feature","features","option","options",
    "thing","stuff","item","items","product","products","device","devices",
    "model","models","series","line","brand","brands",

    # short tokens
    "nah","yup","wow","ugh","meh","ayy","ehh",

    # from your list
    "not","and","but","you","your","this","pro","for","new","one","get",
    "just","like","iphone","phone","message","read"
}

def get_clean_topic_label(topic_id):
    if topic_id == -1:
        return "Misc"
    words = topic_model.get_topic(topic_id)
    if not words:
        return "Misc"
    clean_words = [w for w, _ in words if w.lower() not in JUNK_WORDS and len(w) > 2]
    if not clean_words:
        return words[0][0]
    return " ".join(clean_words[:3])

def extract_probs(probs):
    """Safely handle probs whether float or array."""
    out = []
    for p in probs:
        if p is None:
            out.append(0.0)
        elif isinstance(p, (list, tuple)) or hasattr(p, "__iter__"):
            out.append(float(max(p)))
        else:
            out.append(float(p))
    return out

def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)

    try:
        # --- Get new reviews ---
        df = pd.read_sql("""
            SELECT r.id, r.text
            FROM reviews_raw r
            LEFT JOIN reviews_processed p ON p.review_id = r.id
            WHERE p.review_id IS NULL
            ORDER BY r.id ASC
            LIMIT 500
        """, con)

        if df.empty:
            print("No new reviews found.")
            return

        # --- Aspects + Sentiment ---
        aspects, sentiments, scores, signed_scores = [], [], [], []
        for txt in df["text"]:
            aspects.append(extract_aspects(txt))
            lab, sc, signed = analyze_sentiment(txt)
            sentiments.append(lab)
            scores.append(sc)
            signed_scores.append(signed)

        df["aspects"] = [json.dumps(a.split(",")) if a else "[]" for a in aspects]
        df["aspect_csv"] = aspects
        df["sentiment_label"] = sentiments
        df["score"] = scores
        df["score_signed"] = signed_scores

        # --- Topics ---
        topics, probs = topic_model.transform(df["text"].tolist())
        df["topic_id"] = topics
        df["topic_prob"] = extract_probs(probs)
        df["topic_label"] = [get_clean_topic_label(t) for t in topics]
        df["topic_source"] = "bertopic-transform"

        # --- Add processed_at ---
        df["processed_at"] = datetime.utcnow()

        # --- Save (replace table so schema always matches) ---
        df_to_save = (
            df.drop(columns=["text"])
              .rename(columns={"id": "review_id"})
        )
        df_to_save.to_sql("reviews_processed", con, if_exists="append", index=False)

        print(f" Processed {len(df)} reviews with aspects + sentiment + topics")

    finally:
        con.close()

if __name__ == "__main__":
    main()
