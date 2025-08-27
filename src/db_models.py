
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)
Base = declarative_base()

class Review(Base):
    __tablename__ = "reviews_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(32))
    source_id = Column(String(128))
    product_id = Column(String(64), nullable=True)
    brand = Column(String(64), nullable=True)
    author = Column(String(128), nullable=True)
    text = Column(Text)
    rating = Column(Float, nullable=True)
    url = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Processed(Base):
    __tablename__ = "reviews_processed"
    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(Integer)
    sentiment_label = Column(String(8))
    score = Column(Float)
    score_signed = Column(Float)
    aspect_csv = Column(Text)
    topic_label = Column(String(128), nullable=True)

def init_db():
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()
    print("DB initialized at", DATABASE_URL)
