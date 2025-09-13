# realtime/ingest_reddit_stream.py
# Stream comments until a fixed number (default 50), processing every M new saved rows.
# Uses Reddit's own buffer instead of explicit backfill.

import os, re, sys, time, logging
from typing import List, Optional

sys.path.append("/opt/airflow/src")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # local dev

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
log = logging.getLogger("reddit-ingestor")

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

# ---------- expanded keywords ----------
KEYWORDS = _tokens(
    "REDDIT_KEYWORDS",
    "battery,charging,charger,power,fast charge,slow charge,"
    "camera,lens,photo,video,low light,night mode,"
    "screen,display,brightness,resolution,refresh rate,lag,pixel,"
    "performance,speed,crash,bug,smooth,fast,slow,"
    "heating,overheating,temperature,fan noise,"
    "durability,build,design,lightweight,thin,heavy,"
    "price,cost,expensive,cheap,value,worth,"
    "quality,warranty,repair,service,"
    "sound,audio,mic,speaker,bass,volume,"
    "connectivity,wifi,bluetooth,5g,signal,reception,"
    "storage,memory,ram,sd card,expandable,"
    "gaming,fps,graphics,frame rate,gpu,cpu,"
    "packaging,shipping,delivery,return"
)

# ---------- expanded gadget/product terms ----------
PRODUCT_TERMS = _tokens(
    "PRODUCT_TERMS",
    "iphone,ios,ipad,macbook,airpods,apple watch,apple,"
    "android,pixel,galaxy,samsung,oneplus,xiaomi,oppo,vivo,"
    "headphones,earbuds,bluetooth,smart tv,television,oled,qled,"
    "laptop,ultrabook,chromebook,surface,lenovo,thinkpad,dell,hp,asus,"
    "nintendo,playstation,ps5,xbox,console,gaming pc,"
    "camera,canon,nikon,sony alpha,gopro,dslr,mirrorless,"
    "tablet,kindle,e-reader,"
    "drone,dji,smartwatch,fitbit,garmin,wearable,"
    "router,wifi,mesh,smart speaker,echo,google home,alexa"
)

# Match mode (keep AND)
MATCH_MODE = os.getenv("REDDIT_MATCH_MODE", "AND").upper()
if MATCH_MODE not in {"AND", "OR"}:
    MATCH_MODE = "AND"

# knobs
REALTIME_BATCH = int(os.getenv("REDDIT_REALTIME_BATCH", "5"))      # process after this many
STREAM_COMMENTS = int(os.getenv("REDDIT_STREAM_COMMENTS", "50"))  # stop after N comments (default 50)

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
    author = str(comment.author) if comment.author else "[deleted]"
    a = author.lower()

    if a.endswith("bot") or a in {"automoderator", "bot"}:
        return False
    if not body or not body.strip():
        return False

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

# ---------- Stream until N comments ----------
def stream_and_process(reddit, session, max_comments: int = STREAM_COMMENTS):
    sr = "+".join(SUBREDDITS) if SUBREDDITS else "all"
    log.info("Streaming from: %s | match=%s | keywords=%s | products=%s",
             sr, MATCH_MODE, len(KEYWORDS), len(PRODUCT_TERMS))

    saved_since_last_process = 0
    total_saved = 0

    # Use skip_existing=False → pulls Reddit's buffer first, then live
    stream = reddit.subreddit(sr).stream.comments(skip_existing=False)
    for comment in stream:
        if total_saved >= max_comments:
            log.info("Reached %d comments, stopping stream.", max_comments)
            break
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
                total_saved += 1

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
        stream_and_process(reddit, session)
    except KeyboardInterrupt:
        log.info("Shutting down (Ctrl+C).")
    finally:
        session.close()
        log.info("DB session closed.")

if __name__ == "__main__":
    main()
