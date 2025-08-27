# tools/enrich_brand_product.py
import os, re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")
engine = create_engine(DB_URL, future=True)

# Very simple brand/model extractors — tune patterns as needed
BRAND_PATTERNS = {
    "Apple":   r"\b(iphone|ios|ipad|macbook|airpods|apple watch|apple)\b",
    "Samsung": r"\b(galaxy s\d{2}|galaxy|samsung)\b",
    "Google":  r"\b(pixel \d+(?: pro)?|pixel|google)\b",
    "OnePlus": r"\b(oneplus \d+|oneplus)\b",
    "Xiaomi":  r"\b(xiaomi \d+|xiaomi|mi \d+)\b",
}

MODEL_PATTERNS = [
    r"(iphone\s?(?:\d{1,2}|se|pro(?: max)?))",
    r"(galaxy\s?s\d{1,2}\s?(?:ultra|plus)?)",
    r"(pixel\s?\d{1,2}\s?(?:pro)?)",
    r"(oneplus\s?\d{1,2})",
    r"(xiaomi\s?\d{1,2})",
    r"(macbook\s?(?:air|pro)\s?(?:\d{4})?)",
    r"(ipad\s?(?:pro|air|mini)?)",
]

BRAND_RES = {b: re.compile(p, re.I) for b,p in BRAND_PATTERNS.items()}
MODEL_RES = [re.compile(p, re.I) for p in MODEL_PATTERNS]

def extract_brand(text: str):
    t = text or ""
    for brand, rx in BRAND_RES.items():
        if rx.search(t): return brand
    return None

def extract_model(text: str):
    t = text or ""
    for rx in MODEL_RES:
        m = rx.search(t)
        if m: return m.group(1).strip()
    return None

def main():
    updated = 0
    with engine.begin() as con:
        rows = con.execute(text("""
            SELECT id, text, brand, product_id
            FROM reviews_raw
            WHERE source='reddit'
              AND (brand IS NULL OR brand = '' OR product_id IS NULL OR product_id = '')
        """)).mappings().all()

    if not rows:
        print("Nothing to enrich.")
        return

    with engine.begin() as con:
        for r in rows:
            text_ = r["text"] or ""
            brand = r["brand"] or extract_brand(text_)
            model = r["product_id"] or extract_model(text_)
            if brand or model:
                con.execute(text("""
                    UPDATE reviews_raw
                    SET brand = COALESCE(:brand, brand),
                        product_id = COALESCE(:model, product_id)
                    WHERE id = :id
                """), {"brand": brand, "model": model, "id": r["id"]})
                updated += 1

    print(f"Enriched {updated} rows (brand/product_id).")

if __name__ == "__main__":
    main()
