import os
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Database URL
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/aspect_reviews.db")

# SQLAlchemy setup
engine = create_engine(DB_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# --- Tables ---

class Review(Base):
    __tablename__ = "reviews_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)
    source_id = Column(String(100), unique=True, nullable=False)
    author = Column(String(100))
    text = Column(Text)
    url = Column(String(300))
    created_at = Column(DateTime, default=datetime.utcnow)

    processed = relationship("Processed", back_populates="review", uselist=False)

class Processed(Base):
    __tablename__ = "reviews_processed"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(Integer, ForeignKey("reviews_raw.id"), nullable=False, unique=True)

    # Phase 2 – Aspects
    aspects = Column(Text)
    aspect_csv = Column(Text)

    # Phase 2/3 – Sentiment
    sentiment_label = Column(String(10))
    score = Column(Float)
    score_signed = Column(Float)

    # Phase 3 – Topics
    topic_id = Column(Integer)
    topic_label = Column(String(200))
    topic_prob = Column(Float)         # NEW: probability score for topic
    topic_source = Column(String(50))  # NEW: where topic came from ("bertopic")

    processed_at = Column(DateTime, default=datetime.utcnow)

    review = relationship("Review", back_populates="processed")

# --- Helper ---
def init_db():
    Base.metadata.create_all(bind=engine)