"""
FastAPI application: routes, scheduler, and startup.
"""
import json
import logging
import os
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db, init_db
from emailer import send_daily_digest
from models import Project
from scraper import run_full_scrape, run_news_scrape

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="US Natural Gas Plant Tracker", version="1.0.0")

# ---------------------------------------------------------------------------
# Startup: DB init + scheduler
# ---------------------------------------------------------------------------

scheduler = BackgroundScheduler(timezone="America/New_York")


@app.on_event("startup")
async def startup():
    init_db()
    logger.info("Database initialized.")

    # Daily 6am ET: EIA + FERC scrape
    scheduler.add_job(
        _scheduled_scrape,
        CronTrigger(hour=6, minute=0),
        id="full_scrape",
        replace_existing=True,
    )
    # Daily 6:30am ET: news OEM/EPC
    scheduler.add_job(
        _scheduled_news,
        CronTrigger(hour=6, minute=30),
        id="news_scrape",
        replace_existing=True,
    )
    # Daily 7am ET: email digest
    scheduler.add_job(
        _scheduled_email,
        CronTrigger(hour=7, minute=0),
        id="email_digest",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started (scrape@6am, news@6:30am, email@7am ET).")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown(wait=False)


def _scheduled_scrape():
    from database import SessionLocal
    db = SessionLocal()
    try:
        result = run_full_scrape(db)
        logger.info("Scheduled scrape complete: %s", result)
    finally:
        db.close()


def _scheduled_news():
    from database import SessionLocal
    db = SessionLocal()
    try:
        result = run_news_scrape(db)
        logger.info("Scheduled news scrape: %s", result)
    finally:
        db.close()


def _scheduled_email():
    from database import SessionLocal
    db = SessionLocal()
    try:
        ok = send_daily_digest(db)
        logger.info("Email digest sent: %s", ok)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse("static/index.html")


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ProjectCreate(BaseModel):
    name: str
    state: Optional[str] = None
    county: Optional[str] = None
    capacity_mw: Optional[float] = None
    fuel_type: Optional[str] = "Natural Gas"
    technology: Optional[str] = None
    developer: Optional[str] = None
    oem: Optional[str] = None
    epc: Optional[str] = None
    status: Optional[str] = "Planned"
    proposed_cod: Optional[str] = None
    eia_plant_id: Optional[str] = None
    ferc_queue_id: Optional[str] = None
    source: Optional[str] = "Manual"
    notes: Optional[str] = None


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    capacity_mw: Optional[float] = None
    fuel_type: Optional[str] = None
    technology: Optional[str] = None
    developer: Optional[str] = None
    oem: Optional[str] = None
    epc: Optional[str] = None
    status: Optional[str] = None
    proposed_cod: Optional[str] = None
    eia_plant_id: Optional[str] = None
    ferc_queue_id: Optional[str] = None
    source: Optional[str] = None
    notes: Optional[str] = None
    news_links: Optional[str] = None
    cost_per_kw: Optional[float] = None
    cost_source_url: Optional[str] = None


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

def _project_to_dict(p: Project) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "state": p.state,
        "county": p.county,
        "capacity_mw": p.capacity_mw,
        "fuel_type": p.fuel_type,
        "technology": p.technology,
        "developer": p.developer,
        "oem": p.oem,
        "epc": p.epc,
        "status": p.status,
        "proposed_cod": p.proposed_cod,
        "eia_plant_id": p.eia_plant_id,
        "ferc_queue_id": p.ferc_queue_id,
        "source": p.source,
        "news_links": json.loads(p.news_links or "[]"),
        "notes": p.notes,
        "cost_per_kw": p.cost_per_kw,
        "cost_source_url": p.cost_source_url,
        "last_updated": p.last_updated.isoformat() if p.last_updated else None,
        "created_at": p.created_at.isoformat() if p.created_at else None,
    }


@app.get("/api/projects")
def list_projects(
    state: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(Project)
    if state:
        q = q.filter(Project.state == state)
    if status:
        q = q.filter(Project.status == status)
    if search:
        term = f"%{search}%"
        q = q.filter(
            Project.name.ilike(term)
            | Project.developer.ilike(term)
            | Project.oem.ilike(term)
            | Project.epc.ilike(term)
            | Project.county.ilike(term)
        )
    total = q.count()
    projects = q.order_by(Project.capacity_mw.desc()).offset(skip).limit(limit).all()
    return {"total": total, "projects": [_project_to_dict(p) for p in projects]}


@app.get("/api/projects/{project_id}")
def get_project(project_id: int, db: Session = Depends(get_db)):
    p = db.query(Project).filter(Project.id == project_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Project not found")
    return _project_to_dict(p)


@app.post("/api/projects", status_code=201)
def create_project(data: ProjectCreate, db: Session = Depends(get_db)):
    project = Project(**data.model_dump(exclude_none=True), news_links="[]")
    project.created_at = datetime.utcnow()
    project.last_updated = datetime.utcnow()
    db.add(project)
    db.commit()
    db.refresh(project)
    return _project_to_dict(project)


@app.put("/api/projects/{project_id}")
def update_project(project_id: int, data: ProjectUpdate, db: Session = Depends(get_db)):
    p = db.query(Project).filter(Project.id == project_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Project not found")
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(p, field, value)
    p.last_updated = datetime.utcnow()
    db.commit()
    db.refresh(p)
    return _project_to_dict(p)


@app.delete("/api/projects/{project_id}", status_code=204)
def delete_project(project_id: int, db: Session = Depends(get_db)):
    p = db.query(Project).filter(Project.id == project_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Project not found")
    db.delete(p)
    db.commit()


@app.post("/api/scrape")
def trigger_scrape(db: Session = Depends(get_db)):
    result = run_full_scrape(db)
    return {"status": "ok", "result": result}


@app.post("/api/scrape/news")
def trigger_news_scrape(db: Session = Depends(get_db)):
    result = run_news_scrape(db)
    return {"status": "ok", "result": result}


@app.post("/api/test-email")
def test_email(db: Session = Depends(get_db)):
    ok = send_daily_digest(db)
    return {"status": "sent" if ok else "failed"}


@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)):
    total = db.query(Project).count()
    under_construction = db.query(Project).filter(
        Project.status.ilike("%construction%")
    ).count()
    planned = db.query(Project).filter(
        Project.status.ilike("%planned%")
    ).count()

    total_mw_row = db.query(Project).all()
    total_mw = sum(p.capacity_mw or 0 for p in total_mw_row)

    missing_oem = db.query(Project).filter(Project.oem == None).count()  # noqa: E711
    missing_epc = db.query(Project).filter(Project.epc == None).count()  # noqa: E711

    states = {}
    for p in db.query(Project).all():
        if p.state:
            states[p.state] = states.get(p.state, 0) + 1
    top_states = sorted(states.items(), key=lambda x: -x[1])[:10]

    return {
        "total_projects": total,
        "under_construction": under_construction,
        "planned": planned,
        "total_mw": round(total_mw, 0),
        "missing_oem": missing_oem,
        "missing_epc": missing_epc,
        "top_states": top_states,
    }
