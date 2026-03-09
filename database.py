import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_PATH = os.environ.get("DB_PATH", "natgas.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    import sqlite3
    import logging
    logger = logging.getLogger(__name__)

    from models import Project  # noqa: F401 — registers model with Base
    Base.metadata.create_all(bind=engine)

    # Add any new columns to the existing table (SQLite doesn't support ALTER TABLE … ADD COLUMN IF NOT EXISTS)
    new_cols = [
        ("cost_per_kw", "REAL"),
        ("cost_source_url", "VARCHAR"),
    ]
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(projects)")
        existing = {row[1] for row in cur.fetchall()}
        for col_name, col_type in new_cols:
            if col_name not in existing:
                cur.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
                logger.info("Migrated: added column %s to projects", col_name)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("DB migration check failed: %s", e)
