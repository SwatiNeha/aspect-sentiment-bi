import os
import sys
import argparse
from typing import List

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.engine import make_url

# Try to import your models for optional --recreate
Base = None
init_db = None
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from src.db_models import Base as _Base, init_db as _init_db
    Base = _Base
    init_db = _init_db
except Exception:
    pass

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")

ap = argparse.ArgumentParser(description="Dangerous: remove everything from DB.")
ap.add_argument("--dry-run", action="store_true", help="Show what would be dropped/deleted.")
ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
ap.add_argument("--delete-file", action="store_true", help="(SQLite only) delete the .db file.")
ap.add_argument("--recreate", action="store_true", help="After nuking, recreate empty tables (uses src.db_models).")
args = ap.parse_args()

def confirm_or_exit():
    if args.dry_run or args.yes:
        return
    print("⚠️  This will REMOVE ALL DATA from your database:")
    print(f"    DATABASE_URL = {DB_URL}")
    if args.delete_file:
        print("    Mode: DELETE SQLite FILE")
    else:
        print("    Mode: DROP ALL TABLES")
    ans = input("Type NUKE to proceed: ").strip()
    if ans != "NUKE":
        print("Cancelled.")
        sys.exit(0)

def sqlite_file_path(db_url: str) -> str:
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        return ""
    # url.database can be relative or absolute
    db_path = url.database or ""
    if not db_path:
        return ""
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(os.path.join(os.getcwd(), db_path))
    return db_path

def drop_all_tables(db_url: str, dry_run: bool = False) -> List[str]:
    engine = create_engine(db_url, future=True)
    meta = MetaData()
    meta.reflect(bind=engine)
    tables = list(meta.tables.keys())
    if dry_run:
        print("[DRY RUN] Would drop tables:", tables or "(none)")
        engine.dispose()
        return tables
    if not tables:
        print("No tables found; nothing to drop.")
        engine.dispose()
        return tables
    # For Postgres, dropping all tables via MetaData is fine. If permissions cause issues,
    # you could fallback to dropping the public schema (not implemented by default).
    meta.drop_all(bind=engine)
    engine.dispose()
    print("✓ Dropped tables:", tables)
    return tables

def delete_sqlite_file(db_url: str, dry_run: bool = False):
    path = sqlite_file_path(db_url)
    if not path:
        print("Not a SQLite file database; --delete-file is ignored.")
        return
    if not os.path.exists(path):
        print(f"SQLite file not found (nothing to delete): {path}")
        return
    if dry_run:
        print(f"[DRY RUN] Would delete SQLite file: {path}")
        return
    # Make sure no open connections
    try:
        create_engine(db_url).dispose()
    except Exception:
        pass
    os.remove(path)
    print(f"✓ Deleted SQLite file: {path}")

def recreate_schema(db_url: str):
    if init_db:
        try:
            init_db()
            print("✓ Recreated empty schema via src.db_models.init_db()")
            return
        except Exception as e:
            print(f"init_db() failed: {e}; falling back to Base.metadata if available")
    if Base is not None:
        try:
            engine = create_engine(db_url, future=True)
            Base.metadata.create_all(engine)
            engine.dispose()
            print("✓ Recreated empty schema via Base.metadata.create_all()")
            return
        except Exception as e:
            print(f"Base.metadata.create_all failed: {e}")
    print("! Could not recreate schema (no models found). Database remains empty.")

def main():
    print("— DATABASE_URL:", DB_URL)
    mode = "DELETE FILE (SQLite)" if args.delete_file else "DROP ALL TABLES"
    print("— Mode:", mode)
    confirm_or_exit()

    if args.delete_file:
        delete_sqlite_file(DB_URL, dry_run=args.dry_run)
    else:
        drop_all_tables(DB_URL, dry_run=args.dry_run)

    if args.recreate and not args.dry_run:
        recreate_schema(DB_URL)

    if args.dry_run:
        print("DRY RUN complete. No changes applied.")
    else:
        print("Done.")

if __name__ == "__main__":
    main()