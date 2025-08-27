# realtime/hybrid_ingest_and_process.py
# Backfill the last N comments ACROSS ALL selected subreddits (not per sub),
# then stream new comments and run processing every M saved rows.

import os, re, sys, time, logging
from typing import List, Optional

# allow "from src..." when running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
import praw
from src.db_models import SessionLocal, Review, init_db

# Optional: trigger the mini NLP pipeline after each mini-batch
try:
    from realtime.process_new import main as process_batch
except Exception:
    process_batch = None

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hybrid-ingestor")

# ---------- env ----------
load_dotenv()
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
UA = os.getenv("REDDIT_USER_AGENT", "aspect-sentiment-bi/0.1")

SUBREDDITS = [
    s.strip() for s in os.getenv("REDDIT_SUBREDDITS", "iphone,Android,gadgets").split(",") if s.strip()
]

def _tokens(env_key: str, default_csv: str, min_len: int = 3) -> List[str]:
    vals = [w.strip() for w in os.getenv(env_key, default_csv).split(",")]
    return [w for w in vals if len(w) >= min_len]

KEYWORDS = _tokens("REDDIT_KEYWORDS", "battery,camera,screen,shipping,packaging,quality,price")
PRODUCT_TERMS = _tokens(
    "PRODUCT_TERMS",
    "iphone,ios,ipad,macbook,airpods,apple watch,apple,android,pixel,galaxy,samsung,oneplus,xiaomi"
)
MATCH_MODE = os.getenv("REDDIT_MATCH_MODE", "AND").upper()
if MATCH_MODE not in {"AND", "OR"}:
    MATCH_MODE = "AND"

# <<< your requested knobs >>>
BACKFILL_TOTAL = int(os.getenv("REDDIT_BACKFILL_TOTAL", "20"))   # overall, across all subs
REALTIME_BATCH = int(os.getenv("REDDIT_REALTIME_BATCH", "5"))    # process after this many new
OVERSAMPLE = int(os.getenv("REDDIT_BACKFILL_OVERSAMPLE", "10"))  # fetch more to survive filtering

def _compile_or(words: List[str]) -> Optional[re.Pattern]:
    if not words:
        return None
    return re.compile(r"\b(" + "|".join(map(re.escape, words)) + r")\b", re.I)

KW_RE   = _compile_or(KEYWORDS)
PROD_RE = _compile_or(PRODUCT_TERMS)

def should_keep(text: str, submission_title: str = "") -> bool:
    blob = f"{submission_title}\n{text or ''}"
    kw_hit = (KW_RE.search(blob) is not None) if KW_RE else True
    prod_hit = (PROD_RE.search(blob) is not None) if PROD_RE else True
    return (kw_hit and prod_hit) if MATCH_MODE == "AND" else (kw_hit or prod_hit)

def create_reddit() -> praw.Reddit:
    if not CLIENT_ID or not CLIENT_SECRET or not UA:
        log.error("Missing Reddit credentials. Check .env")
        sys.exit(1)
    return praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=UA)

# ---------- DB insert helper ----------
def save_comment(session: SessionLocal, comment, body: str, title: str) -> bool:
    # author string
    author = str(comment.author) if comment.author else "[deleted]"
    a = author.lower()

    # skip obvious bots (AutoModerator, Nightbot, etc.)
    if a.endswith("bot") or a in {"automoderator", "bot"}:
        return False

    # skip empty text
    if not body or not body.strip():
        return False

    # de-dup on (source, source_id)
    exists = session.query(Review.id).filter_by(source="reddit", source_id=comment.id).first()
    if exists:
        return False

    url = f"https://www.reddit.com{comment.permalink}"
    rec = Review(
        source="reddit",
        source_id=comment.id,
        author=author,
        text=body,
        url=url,
    )
    session.add(rec)
    session.commit()
    log.info("Saved %s | u/%s | %s", comment.id, author, url)
    return True

# ---------- Backfill (overall N, not per sub) ----------
def backfill_recent_total(reddit, session):
    if BACKFILL_TOTAL <= 0:
        return 0
    sr = "+".join(SUBREDDITS) if SUBREDDITS else "all"
    log.info("Backfilling last %d comments overall from: %s", BACKFILL_TOTAL, sr)

    saved = 0
    # oversample to account for filters; break once we save BACKFILL_TOTAL
    raw_limit = max(BACKFILL_TOTAL * OVERSAMPLE, BACKFILL_TOTAL)
    for c in reddit.subreddit(sr).comments(limit=raw_limit):
        body = c.body or ""
        try:
            title = c.submission.title or ""
        except Exception:
            title = ""
        if not should_keep(body, title):
            continue
        if save_comment(session, c, body, title):
            saved += 1
            if saved >= BACKFILL_TOTAL:
                break
        time.sleep(0.02)  # light throttle
    log.info("Backfill saved %d comments.", saved)
    return saved

# ---------- Stream ----------
def stream_and_process(reddit, session):
    sr = "+".join(SUBREDDITS) if SUBREDDITS else "all"
    log.info("Streaming from: %s | match=%s | keywords=%s | products=%s",
             sr, MATCH_MODE, KEYWORDS, PRODUCT_TERMS)

    saved_since_last_process = 0
    stream = reddit.subreddit(sr).stream.comments(skip_existing=True)
    for comment in stream:
        try:
            body = comment.body or ""
            try:
                title = comment.submission.title or ""
            except Exception:
                title = ""
            if not should_keep(body, title):
                continue
            if save_comment(session, comment, body, title):
                saved_since_last_process += 1

            if REALTIME_BATCH > 0 and saved_since_last_process >= REALTIME_BATCH:
                if process_batch:
                    log.info("Processing mini-batch of %d new rows...", saved_since_last_process)
                    try:
                        process_batch()
                    except Exception as e:
                        log.warning("Processing error: %s", e)
                saved_since_last_process = 0
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.warning("Error handling comment: %s", e)
            session.rollback()
            time.sleep(1)

def main():
    init_db()
    reddit = create_reddit()
    session = SessionLocal()
    try:
        backfill_recent_total(reddit, session)   # overall N (default 20)
        stream_and_process(reddit, session)      # process every M new (default 5)
    except KeyboardInterrupt:
        log.info("Shutting down (Ctrl+C).")
    finally:
        session.close()
        log.info("DB session closed.")

if __name__ == "__main__":
    main()
