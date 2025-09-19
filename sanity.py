import os, sys
from pathlib import Path
from dotenv import load_dotenv
import praw

# load .env from the current folder
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

cid = os.getenv("REDDIT_CLIENT_ID")
csec = os.getenv("REDDIT_CLIENT_SECRET")
ua = os.getenv("REDDIT_USER_AGENT")

assert cid and csec and ua, f"Missing vars -> client_id={cid}, client_secret={bool(csec)}, user_agent={ua}"

r = praw.Reddit(client_id=cid, client_secret=csec, user_agent=ua)
sub = r.subreddit("gadgets")
print("OK. Example hot post title:", next(iter(sub.hot(limit=1))).title)