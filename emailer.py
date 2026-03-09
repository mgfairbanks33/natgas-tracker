"""
Daily email digest via SendGrid.
"""
import json
import logging
import os
from datetime import datetime, timedelta

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy.orm import Session

from models import Project

logger = logging.getLogger(__name__)

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")


def send_daily_digest(db: Session) -> bool:
    """Build and send the daily digest. Returns True on success."""
    if not all([SENDGRID_API_KEY, EMAIL_TO, EMAIL_FROM]):
        logger.warning("SendGrid env vars not configured; skipping email.")
        return False

    yesterday = datetime.utcnow() - timedelta(days=1)

    new_projects = (
        db.query(Project)
        .filter(Project.created_at >= yesterday)
        .order_by(Project.capacity_mw.desc())
        .all()
    )

    updated_projects = (
        db.query(Project)
        .filter(Project.last_updated >= yesterday, Project.created_at < yesterday)
        .order_by(Project.capacity_mw.desc())
        .all()
    )

    top_projects = (
        db.query(Project)
        .order_by(Project.capacity_mw.desc())
        .limit(10)
        .all()
    )

    oem_epc_updates = [
        p for p in updated_projects
        if p.oem or p.epc
    ]

    html = _build_html(new_projects, updated_projects, top_projects, oem_epc_updates)

    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=EMAIL_TO,
        subject=f"US Natural Gas Tracker — Daily Digest {datetime.utcnow().strftime('%Y-%m-%d')}",
        html_content=html,
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info("Email sent: status %s", response.status_code)
        return response.status_code in (200, 202)
    except Exception as e:
        logger.error("SendGrid error: %s", e)
        return False


def _build_html(new_projects, updated_projects, top_projects, oem_epc_updates) -> str:
    style = """
    <style>
      body { font-family: Arial, sans-serif; background: #0f1117; color: #e0e0e0; margin: 0; padding: 20px; }
      h1 { color: #f97316; font-size: 22px; }
      h2 { color: #fb923c; font-size: 16px; border-bottom: 1px solid #333; padding-bottom: 6px; }
      table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
      th { background: #1e2130; color: #f97316; text-align: left; padding: 8px 10px; font-size: 12px; }
      td { padding: 7px 10px; font-size: 12px; border-bottom: 1px solid #1e2130; }
      tr:hover { background: #1a1d2e; }
      .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; }
      .construction { background: #7c3aed22; color: #a78bfa; }
      .planned { background: #0369a122; color: #38bdf8; }
      .none { color: #555; font-style: italic; }
      a { color: #f97316; }
    </style>
    """

    def status_badge(s):
        if not s:
            return '<span class="none">—</span>'
        cls = "construction" if "construction" in s.lower() else "planned"
        return f'<span class="badge {cls}">{s}</span>'

    def val(v):
        return v if v and str(v).lower() not in ("none", "nan", "") else "—"

    def project_row(p):
        mw = f"{p.capacity_mw:,.0f}" if p.capacity_mw else "—"
        return (
            f"<tr><td>{val(p.name)}</td><td>{val(p.state)}</td>"
            f"<td>{mw}</td><td>{val(p.developer)}</td>"
            f"<td>{val(p.oem)}</td><td>{val(p.epc)}</td>"
            f"<td>{status_badge(p.status)}</td><td>{val(p.proposed_cod)}</td></tr>"
        )

    table_header = (
        "<tr><th>Project</th><th>State</th><th>MW</th><th>Developer</th>"
        "<th>OEM</th><th>EPC</th><th>Status</th><th>COD</th></tr>"
    )

    sections = []

    # New projects
    if new_projects:
        rows = "".join(project_row(p) for p in new_projects)
        sections.append(
            f"<h2>New Projects ({len(new_projects)})</h2>"
            f"<table>{table_header}{rows}</table>"
        )
    else:
        sections.append("<h2>New Projects</h2><p class='none'>None added since yesterday.</p>")

    # Status changes
    if updated_projects:
        rows = "".join(project_row(p) for p in updated_projects)
        sections.append(
            f"<h2>Status Updates ({len(updated_projects)})</h2>"
            f"<table>{table_header}{rows}</table>"
        )

    # OEM/EPC updates
    if oem_epc_updates:
        rows = "".join(project_row(p) for p in oem_epc_updates)
        sections.append(
            f"<h2>OEM / EPC Updates ({len(oem_epc_updates)})</h2>"
            f"<table>{table_header}{rows}</table>"
        )

    # Top 10 by MW
    rows = "".join(project_row(p) for p in top_projects)
    sections.append(
        f"<h2>Top 10 Projects by MW</h2>"
        f"<table>{table_header}{rows}</table>"
    )

    body = "\n".join(sections)
    date_str = datetime.utcnow().strftime("%B %d, %Y")

    return f"""<!DOCTYPE html>
<html><head>{style}</head>
<body>
  <h1>US Natural Gas Plant Tracker — {date_str}</h1>
  {body}
  <p style="color:#555;font-size:11px;margin-top:30px;">
    Generated automatically. Data sources: EIA Form 860M, FERC interconnection queues, public news.
  </p>
</body></html>"""
