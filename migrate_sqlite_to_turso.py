import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_models import Review, Processed, Base



# --- SQLite source ---
sqlite_url = "sqlite:///data/aspect_reviews.db"
sqlite_engine = create_engine(sqlite_url)
SQLiteSession = sessionmaker(bind=sqlite_engine)
sqlite_session = SQLiteSession()

# --- Turso target ---
turso_url = os.getenv("DATABASE_URL")  # from .env
auth_token = os.getenv("DATABASE_AUTH_TOKEN")

if auth_token and turso_url.startswith("libsql://"):
    turso_url = f"{turso_url}?authToken={auth_token}"

turso_engine = create_engine(turso_url)
TursoSession = sessionmaker(bind=turso_engine)
turso_session = TursoSession()

# Ensure schema exists on Turso
Base.metadata.create_all(bind=turso_engine)

# --- migrate data ---
print("Migrating reviews...")
for review in sqlite_session.query(Review).all():
    turso_session.merge(review)

print("Migrating processed reviews...")
for proc in sqlite_session.query(Processed).all():
    turso_session.merge(proc)

turso_session.commit()
print("✅ Migration complete!")
