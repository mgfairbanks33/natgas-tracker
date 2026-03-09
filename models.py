from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Text, DateTime
from database import Base


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    state = Column(String)
    county = Column(String)
    capacity_mw = Column(Float)
    fuel_type = Column(String)
    technology = Column(String)
    developer = Column(String)
    oem = Column(String)
    epc = Column(String)
    status = Column(String)
    proposed_cod = Column(String)
    eia_plant_id = Column(String, unique=True, index=True)
    ferc_queue_id = Column(String, index=True)
    source = Column(String, default="Manual")
    news_links = Column(Text, default="[]")  # JSON array of URLs
    notes = Column(Text)
    cost_per_kw = Column(Float)           # reported $/kW from news/filings
    cost_source_url = Column(String)      # URL of the filing/article
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
