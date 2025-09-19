from src.db_models import SessionLocal, Review

session = SessionLocal()
try:
    rows = (
        session.query(Review)
        .filter(Review.source == "reddit")
        .order_by(Review.id.desc())
        .limit(50)
        .all()
    )
    for r in rows:
        print(f"[{r.created_at}] u/{r.author}")
        print(r.text)     # full comment text
        print(r.url)      # permalink
        print("-" * 80)
finally:
    session.close()
