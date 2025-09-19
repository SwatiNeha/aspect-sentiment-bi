import os, re, sys, time, logging
from typing import List, Optional

# allow "from src..." when running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from googleapiclient.discovery import build
from src.db_models import SessionLocal, Review, init_db

# Optional: trigger the processing step after each mini-batch
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
log = logging.getLogger("yt-ingestor")

# ---------- env ----------
load_dotenv()
API_KEY     = os.getenv("YOUTUBE_API_KEY")
VIDEO_ID    = os.getenv("YOUTUBE_VIDEO_ID", "").strip()
CHANNEL_ID  = os.getenv("YOUTUBE_CHANNEL_ID", "").strip()

def _tokens(env_key: str, default_csv: str, min_len: int = 3) -> List[str]:
    vals = [w.strip() for w in os.getenv(env_key, default_csv).split(",")]
    return [w for w in vals if len(w) >= min_len]

KEYWORDS = _tokens("REDDIT_KEYWORDS", "battery,camera,screen,shipping,packaging,quality,price")
PRODUCT_TERMS = _tokens("PRODUCT_TERMS",
    "iphone,ios,ipad,macbook,airpods,apple watch,apple,android,pixel,galaxy,samsung,oneplus,xiaomi")
MATCH_MODE = os.getenv("REDDIT_MATCH_MODE", "AND").upper()
if MATCH_MODE not in {"AND", "OR"}:
    MATCH_MODE = "AND"

BACKFILL_TOTAL   = int(os.getenv("YOUTUBE_BACKFILL_TOTAL", "20"))
OVERSAMPLE       = int(os.getenv("YOUTUBE_BACKFILL_OVERSAMPLE", "8"))
REALTIME_BATCH   = int(os.getenv("YOUTUBE_REALTIME_BATCH", "5"))
POLL_SECONDS     = int(os.getenv("YOUTUBE_POLL_SECONDS", "60"))
YOUTUBE_MAX_PAGES = int(os.getenv("YOUTUBE_MAX_PAGES", "3"))

def _compile_or(words: List[str]) -> Optional[re.Pattern]:
    if not words: return None
    return re.compile(r"\b(" + "|".join(map(re.escape, words)) + r")\b", re.I)

KW_RE   = _compile_or(KEYWORDS)
PROD_RE = _compile_or(PRODUCT_TERMS)

def should_keep(text: str, title: str = "") -> bool:
    blob = f"{title}\n{text or ''}"
    kw_hit = (KW_RE.search(blob)   is not None) if KW_RE   else True
    pr_hit = (PROD_RE.search(blob) is not None) if PROD_RE else True
    return (kw_hit and pr_hit) if MATCH_MODE == "AND" else (kw_hit or pr_hit)

# ---------- YouTube client ----------
def create_youtube():
    if not API_KEY:
        raise SystemExit("Missing YOUTUBE_API_KEY")
    if not (VIDEO_ID or CHANNEL_ID):
        raise SystemExit("Provide YOUTUBE_VIDEO_ID or YOUTUBE_CHANNEL_ID in .env")
    return build("youtube", "v3", developerKey=API_KEY)

# ---------- DB insert helper ----------
def save_comment(session: SessionLocal, cid: str, author: str, text: str, video_id: str) -> bool:
    if session.query(Review).filter_by(source="youtube", source_id=cid).first():
        return False
    url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
    rec = Review(source="youtube", source_id=cid, author=author or "", text=text or "", url=url)
    session.add(rec)
    session.commit()
    log.info("Saved youtube comment %s | %s", cid, url)
    return True

# ---------- Helpers to page through commentThreads ----------
def _comment_threads_iter(yt, *, video_id: str = "", channel_id: str = "", raw_limit: int = 100):
    """
    Yields top-level comment thread items newest-first up to raw_limit (across pages).
    """
    params = dict(part="snippet", maxResults=100, order="time", textFormat="plainText")
    if video_id:
        params["videoId"] = video_id
    else:
        params["allThreadsRelatedToChannelId"] = channel_id

    seen = 0
    page = 0
    next_token = None
    while True:
        if next_token:
            params["pageToken"] = next_token
        req = yt.commentThreads().list(**params)
        resp = req.execute()
        items = resp.get("items", []) or []
        for it in items:
            yield it
            seen += 1
            if seen >= raw_limit:
                return
        next_token = resp.get("nextPageToken")
        page += 1
        if not next_token or page >= YOUTUBE_MAX_PAGES:
            return

# ---------- Backfill overall N ----------
def backfill_recent_total(yt, session):
    if BACKFILL_TOTAL <= 0:
        return 0
    raw_limit = max(BACKFILL_TOTAL * OVERSAMPLE, BACKFILL_TOTAL)
    saved = 0

    # If VIDEO_ID provided, backfill from that video; else from the channel
    src_video = VIDEO_ID if VIDEO_ID else ""
    for it in _comment_threads_iter(yt, video_id=VIDEO_ID, channel_id=CHANNEL_ID, raw_limit=raw_limit):
        top = it["snippet"]["topLevelComment"]["snippet"]
        cid = it["snippet"]["topLevelComment"]["id"]
        text = top.get("textDisplay") or top.get("textOriginal") or ""
        author = top.get("authorDisplayName") or ""
        video_id = top.get("videoId") or src_video
        title = it["snippet"].get("videoTitle", "")  # may not always be populated

        if not should_keep(text, title):
            continue
        if save_comment(session, cid, author, text, video_id):
            saved += 1
            if saved >= BACKFILL_TOTAL:
                break
        time.sleep(0.02)  # light throttle

    log.info("Backfill saved %d comments.", saved)
    return saved

# ---------- Poll loop ----------
def poll_and_process(yt, session):
    saved_since_last_process = 0
    log.info(
        "Polling YouTube (%s) every %ss | match=%s | keywords=%s | products=%s",
        f"video={VIDEO_ID}" if VIDEO_ID else f"channel={CHANNEL_ID}",
        POLL_SECONDS, MATCH_MODE, KEYWORDS, PRODUCT_TERMS
    )
    while True:
        try:
            # get the newest few pages each time (newest-first)
            new_saves = 0
            for it in _comment_threads_iter(yt, video_id=VIDEO_ID, channel_id=CHANNEL_ID, raw_limit=100):
                top = it["snippet"]["topLevelComment"]["snippet"]
                cid = it["snippet"]["topLevelComment"]["id"]
                text = top.get("textDisplay") or top.get("textOriginal") or ""
                author = top.get("authorDisplayName") or ""
                video_id = top.get("videoId") or (VIDEO_ID or "")
                title = it["snippet"].get("videoTitle", "")

                if not should_keep(text, title):
                    continue
                if save_comment(session, cid, author, text, video_id):
                    saved_since_last_process += 1
                    new_saves += 1

                # If we’ve already saved this comment in a prior poll, DB de-dup returns False and we just move on

                if REALTIME_BATCH > 0 and saved_since_last_process >= REALTIME_BATCH:
                    if process_batch:
                        log.info("Processing mini-batch of %d new rows...", saved_since_last_process)
                        try:
                            process_batch()
                        except Exception as e:
                            log.warning("Processing error: %s", e)
                    saved_since_last_process = 0

            if new_saves == 0:
                log.info("No new matching comments this round.")
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.warning("Poll error: %s", e)
            session.rollback()
            time.sleep(5)

def main():
    init_db()
    yt = create_youtube()
    session = SessionLocal()
    try:
        backfill_recent_total(yt, session)   # overall N (default 20)
        poll_and_process(yt, session)        # process every M new (default 5)
    finally:
        session.close()
        log.info("DB session closed.")

if __name__ == "__main__":
    main()