"""
Scraper module: EIA Form 860M, FERC interconnection queues, and news-based OEM/EPC lookup.
"""
import io
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

import feedparser
import pandas as pd
import requests
from sqlalchemy.orm import Session

from models import Project

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EIA_860M_INDEX = "https://www.eia.gov/electricity/data/eia860m/"

EIA_GAS_FUEL_CODES = {"NG", "DFO", "RFO"}  # Natural gas, distillate (peakers), residual
EIA_GAS_TECH_KEYWORDS = {
    "combined cycle",
    "combustion turbine",
    "internal combustion",
    "steam turbine",
    "natural gas",
}
EIA_ACTIVE_STATUSES = {"P", "L", "T", "U", "V"}  # planned / under construction

KNOWN_OEMS = [
    "GE Vernova", "GE Aerospace", "General Electric",
    "Siemens Energy", "Siemens",
    "Mitsubishi Power", "MHPS", "Mitsubishi Hitachi",
    "Solar Turbines",
    "Ansaldo Energy", "Ansaldo",
    "Rolls-Royce",
    "Pratt & Whitney",
    "Wärtsilä", "Wartsila", "Wartsil",
    "MAN Energy",
    "Kawasaki",
    "Capstone Turbine",
]

KNOWN_EPCS = [
    # Well-known power EPC contractors — multi-word names preferred (fewer false positives)
    "Gemma Power Systems", "Gemma Power", "Gemma",
    "Kiewit Power", "Kiewit",
    "Zachry Group", "Zachry",
    "Burns & McDonnell", "Burns and McDonnell",
    "Black & Veatch", "Black and Veatch",
    "Fluor Corporation", "Fluor",
    "Bechtel",
    "AECOM",
    "Worley", "WorleyParsons",
    "Sargent & Lundy", "Sargent and Lundy",
    "Jacobs Engineering", "Jacobs",
    "Wood Group",
    "McDermott",
    "Chicago Bridge & Iron", "CB&I",
    "Granite Construction",
    "Mortenson Construction", "Mortenson",
    "Turner Construction",
    "Skanska",
    "Linde Engineering",
    "Hatch Associates", "Hatch",
    "Amec Foster Wheeler",
    "Westinghouse Electric",
    "Day & Zimmermann", "Day and Zimmermann",
    "Quanta Services",
    "SNC-Lavalin",
    "Wanzek Construction",
    "IHI Engineering",
    "Daelim Industrial",
    "The Industrial Company",  # full name only for "TIC"
    "Stantec",
    "Crowley Carbon",
    "NAES Corporation", "NAES",
    "Redbud Power Partners",
    "Power Engineers", "POWER Engineers",
    "Tetra Tech",
    "Smith Energy Services",
    "CCC Group",
    "Calpine Construction",
    "Shiel Sexton",
    "Barton Malow",
    "PCL Industrial",
    "Foster Wheeler",
    "Alstom Power",
    "Doosan",
    "Sumitomo",
]

# Cost extraction patterns
TOTAL_COST_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)\b', re.IGNORECASE
)
COST_PER_KW_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:per\s+)?(?:k[Ww]h?|kilowatt)(?:\s*(?:installed|capacity))?', re.IGNORECASE
)

# EPC_PATTERN: used to find EPC-related context windows in article text
EPC_CONTEXT_PATTERN = re.compile(
    r"(?i)(EPC|engineering.{0,10}procurement.{0,20}construction"
    r"|awarded.{0,20}contract"
    r"|construction\s+contract"
    r"|prime\s+contractor"
    r"|general\s+contractor)",
)

# FERC/ISO queue URLs (CSV / Excel endpoints as of early 2025)
ISO_QUEUE_URLS = {
    "PJM": "https://www.pjm.com/pub/planning/project-queues/pjm_pjm-queues-active.xlsx",
    "MISO": "https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/",
    "CAISO": "https://www.caiso.com/documents/generatorinterconnectionqueue.xlsx",
    "SPP": "https://www.spp.org/engineering/generator-interconnection/GI_Cluster_Queue_Status.csv",
    "ERCOT": "https://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NatGasTracker/1.0; "
        "+https://github.com/mgfai/natgas-tracker)"
    )
}

# ---------------------------------------------------------------------------
# EIA Form 860M
# ---------------------------------------------------------------------------

def _get_eia_xlsx_url() -> Optional[str]:
    """Scrape the EIA 860M page and return the first xlsx URL that serves real spreadsheet bytes."""
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        resp = sess.get(EIA_860M_INDEX, timeout=30)
        resp.raise_for_status()

        # Strip HTML comments so we don't match commented-out links
        html = re.sub(r'<!--.*?-->', '', resp.text, flags=re.DOTALL)

        matches = re.findall(r'href="([^"]+\.xlsx)', html, re.IGNORECASE)
        if not matches:
            matches = re.findall(r'href="([^"]+\.xls)', html, re.IGNORECASE)

        for href in matches:
            url = href if href.startswith("http") else "https://www.eia.gov" + href
            try:
                # Download first 8 bytes; xlsx files start with PK magic bytes (0x50 0x4B)
                r = sess.get(url, timeout=60, stream=True)
                first_bytes = next(r.iter_content(8), b"")
                r.close()
                if first_bytes[:2] == b"PK":  # valid xlsx/zip
                    logger.info("Found valid EIA xlsx: %s", url)
                    return url
                logger.debug("Skipping %s (not xlsx, starts with: %r)", url, first_bytes[:4])
            except Exception as e:
                logger.debug("Could not probe %s: %s", url, e)
                continue
    except Exception as e:
        logger.error("Failed to find EIA 860M URL: %s", e)
    return None


def scrape_eia(db: Session) -> int:
    """Download EIA 860M, parse Planned sheet, upsert gas projects. Returns count upserted."""
    url = _get_eia_xlsx_url()
    if not url:
        logger.error("Could not locate EIA 860M xlsx file.")
        return 0

    logger.info("Downloading EIA 860M from %s", url)
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        # Visit index page first to establish session/cookies
        sess.get(EIA_860M_INDEX, timeout=15)
        resp = sess.get(url, timeout=180)
        resp.raise_for_status()
        if "html" in resp.headers.get("Content-Type", ""):
            logger.error("EIA returned HTML instead of xlsx — URL may have moved.")
            return 0
    except Exception as e:
        logger.error("EIA download failed: %s", e)
        return 0

    try:
        xls = pd.ExcelFile(io.BytesIO(resp.content))
    except Exception as e:
        logger.error("Failed to parse EIA xlsx: %s", e)
        return 0

    # The sheet name varies — look for "Planned" or "Plant"
    sheet = None
    for name in xls.sheet_names:
        if "planned" in name.lower():
            sheet = name
            break
    if sheet is None:
        logger.error("Could not find Planned sheet in EIA 860M. Sheets: %s", xls.sheet_names)
        return 0

    # Auto-detect header row: scan rows 0-5, use first with >5 non-null, non-Unnamed cells
    header_row = 2  # default for current EIA 860M format (as of 2026)
    try:
        for hr in range(0, 6):
            df_probe = pd.read_excel(xls, sheet_name=sheet, header=hr, nrows=1)
            named = [c for c in df_probe.columns if "Unnamed" not in str(c) and str(c) != "nan"]
            if len(named) >= 5:
                header_row = hr
                break
    except Exception:
        pass

    try:
        df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    except Exception as e:
        logger.error("Failed to read EIA sheet: %s", e)
        return 0

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    # Required column subsets (EIA sometimes renames columns)
    col_map = _eia_column_map(df.columns)
    if col_map is None:
        logger.error("Unexpected EIA column structure: %s", [str(c) for c in df.columns[:20]])
        return 0

    # --- Pass 1: parse all rows into a dict keyed by plant_id (aggregate generators) ---
    plants: dict = {}  # plant_id -> aggregated record

    STATUS_RANK = {"L": 5, "T": 4, "U": 3, "P": 2, "V": 1}  # higher = more advanced

    for _, row in df.iterrows():
        try:
            energy_source = str(row.get(col_map["energy_source"], "")).strip().upper()
            technology = str(row.get(col_map["technology"], "")).strip()
            status_code = str(row.get(col_map["status"], "")).strip().upper()
            raw_plant_id = str(row.get(col_map["plant_id"], "")).strip()

            # Filter: must be gas fuel or gas technology keyword
            is_gas_fuel = any(energy_source.startswith(c) for c in EIA_GAS_FUEL_CODES)
            is_gas_tech = any(k in technology.lower() for k in EIA_GAS_TECH_KEYWORDS)
            if not (is_gas_fuel or is_gas_tech):
                continue

            # Filter: active/planned status
            if status_code and status_code not in EIA_ACTIVE_STATUSES:
                if not any(s in status_code for s in ["PLAN", "CONST", "PERMIT"]):
                    continue

            capacity = _safe_float(row.get(col_map["capacity"])) or 0.0
            if capacity < 1:
                continue

            name = str(row.get(col_map["name"], "")).strip()
            state = str(row.get(col_map["state"], "")).strip()
            county = str(row.get(col_map.get("county", ""), "")).strip()
            developer = str(row.get(col_map.get("developer", ""), "")).strip()
            cod_mo = str(row.get(col_map.get("cod_month", ""), "")).strip()
            cod_yr = str(row.get(col_map.get("cod_year", ""), "")).strip()
            cod = (
                f"{cod_mo}/{cod_yr}"
                if cod_mo and cod_yr and cod_mo != "nan" and cod_yr != "nan"
                else (cod_yr if cod_yr and cod_yr != "nan" else "")
            )

            if not name or name.lower() in ("nan", "none", ""):
                continue
            if raw_plant_id == "nan":
                raw_plant_id = ""

            key = raw_plant_id or name  # group all generators of same plant

            if key in plants:
                # Aggregate: sum MW, keep most advanced status
                plants[key]["capacity_mw"] = plants[key]["capacity_mw"] + capacity
                if STATUS_RANK.get(status_code[:1], 0) > STATUS_RANK.get(plants[key]["status_code"][:1], 0):
                    plants[key]["status_code"] = status_code
                    plants[key]["cod"] = cod
            else:
                plants[key] = {
                    "plant_id": raw_plant_id,
                    "name": name,
                    "state": state,
                    "county": county,
                    "capacity_mw": capacity,
                    "fuel_type": energy_source,
                    "technology": technology,
                    "status_code": status_code,
                    "cod": cod,
                    "developer": developer,
                }
        except Exception as e:
            logger.debug("Row parse error: %s", e)
            continue

    # --- Pass 2: upsert aggregated plant records ---
    count = 0
    for key, p in plants.items():
        try:
            existing = (
                db.query(Project).filter(Project.eia_plant_id == p["plant_id"]).first()
                if p["plant_id"]
                else db.query(Project).filter(Project.name == p["name"], Project.state == p["state"]).first()
            )
            mapped_status = _map_eia_status(p["status_code"])
            if existing:
                existing.capacity_mw = p["capacity_mw"]
                existing.fuel_type = p["fuel_type"]
                existing.technology = p["technology"]
                existing.status = mapped_status
                existing.proposed_cod = p["cod"]
                existing.developer = p["developer"] or existing.developer
                existing.last_updated = datetime.utcnow()
            else:
                project = Project(
                    name=p["name"],
                    state=p["state"],
                    county=p["county"],
                    capacity_mw=p["capacity_mw"],
                    fuel_type=p["fuel_type"],
                    technology=p["technology"],
                    status=mapped_status,
                    proposed_cod=p["cod"],
                    developer=p["developer"],
                    eia_plant_id=p["plant_id"] or None,
                    source="EIA",
                    news_links="[]",
                )
                db.add(project)
            count += 1
        except Exception as e:
            logger.debug("Upsert error for %s: %s", p.get("name"), e)
            continue

    db.commit()
    logger.info("EIA scrape: upserted %d gas projects", count)
    return count


def _eia_column_map(columns) -> Optional[dict]:
    """Return a mapping of logical field → actual column name."""
    cols_lower = {c.lower(): c for c in columns}

    def find(keywords):
        for kw in keywords:
            for col_lower, col_orig in cols_lower.items():
                if kw in col_lower:
                    return col_orig
        return None

    # Current EIA 860M column names (as of 2026):
    # Entity ID, Entity Name, Plant ID, Plant Name, Plant State, County,
    # Nameplate Capacity (MW), Technology, Energy Source Code,
    # Generator ID, Planned Operation Month, Planned Operation Year, Status
    plant_id = find(["plant id"])
    generator_id = find(["generator id"])
    name = find(["plant name", "generator name", "entity name"])
    state = find(["plant state", "state"])
    county = find(["county"])
    capacity = find(["nameplate capacity", "nameplate", "summer capacity", "capacity (mw)"])
    energy_source = find(["energy source code", "energy source", "fuel"])
    technology = find(["technology", "prime mover"])
    status = find(["status"])
    developer = find(["entity name", "owner", "developer", "utility name"])
    cod_month = find(["planned operation month", "operation month"])
    cod_year = find(["planned operation year", "operation year", "online year"])

    if not all([name, state, capacity, energy_source, technology, status]):
        return None

    return {
        "plant_id": plant_id or "",
        "generator_id": generator_id or "",
        "name": name,
        "state": state,
        "county": county or "",
        "capacity": capacity,
        "energy_source": energy_source,
        "technology": technology,
        "status": status,
        "developer": developer or "",
        "cod_month": cod_month or "",
        "cod_year": cod_year or "",
    }


def _map_eia_status(code: str) -> str:
    # EIA stores full strings like "(V) UNDER CONSTRUCTION, MORE THAN 50 PERCENT COMPLETE"
    # Extract the letter code from parentheses if present
    m = re.match(r"^\(([A-Z]+)\)", code.strip())
    letter = m.group(1) if m else code.strip().upper()
    mapping = {
        "P": "Planned",
        "L": "Pending Regulatory Approval",
        "T": "Approved, Not Yet Under Construction",
        "U": "Under Construction <50%",
        "V": "Under Construction >50%",
        "TS": "Construction Complete",
        "OT": "Other",
    }
    return mapping.get(letter, code)


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return f if not pd.isna(f) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# FERC / ISO Queue (PJM primary, others best-effort)
# ---------------------------------------------------------------------------

def scrape_ferc_queues(db: Session) -> int:
    """Attempt to pull PJM queue data; other ISOs best-effort. Returns count upserted."""
    count = 0
    count += _scrape_pjm(db)
    return count


def _scrape_pjm(db: Session) -> int:
    url = ISO_QUEUE_URLS["PJM"]
    logger.info("Downloading PJM queue from %s", url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content), header=0)
    except Exception as e:
        logger.warning("PJM queue fetch failed: %s", e)
        return 0

    df.columns = [str(c).strip() for c in df.columns]
    cols_lower = {c.lower(): c for c in df.columns}

    def find(kws):
        for kw in kws:
            for cl, co in cols_lower.items():
                if kw in cl:
                    return co
        return None

    name_col = find(["project name", "name"])
    state_col = find(["state"])
    mw_col = find(["mw ac", "summer capability", "mw"])
    fuel_col = find(["fuel", "energy source"])
    status_col = find(["status", "queue status"])
    queue_col = find(["queue position", "queue id", "queue #"])
    dev_col = find(["developer", "applicant", "owner"])
    cod_col = find(["in service", "cod", "commercial"])

    count = 0
    for _, row in df.iterrows():
        try:
            fuel = str(row.get(fuel_col, "")).upper()
            if fuel_col and not any(g in fuel for g in ["GAS", "NG", "NATURAL"]):
                continue
            queue_id = str(row.get(queue_col, "")).strip() if queue_col else None
            name = str(row.get(name_col, "")).strip() if name_col else None
            if not name or name.lower() in ("nan", "none", ""):
                continue

            existing = None
            if queue_id and queue_id != "nan":
                existing = db.query(Project).filter(Project.ferc_queue_id == queue_id).first()

            capacity = _safe_float(row.get(mw_col)) if mw_col else None
            state = str(row.get(state_col, "")).strip() if state_col else ""
            status = str(row.get(status_col, "")).strip() if status_col else ""
            developer = str(row.get(dev_col, "")).strip() if dev_col else ""
            cod = str(row.get(cod_col, "")).strip() if cod_col else ""

            if existing:
                existing.ferc_queue_id = queue_id
                existing.developer = developer or existing.developer
                existing.last_updated = datetime.utcnow()
            else:
                project = Project(
                    name=name,
                    state=state,
                    capacity_mw=capacity,
                    fuel_type="Natural Gas",
                    status=status or "Planned",
                    proposed_cod=cod,
                    developer=developer,
                    ferc_queue_id=queue_id if queue_id and queue_id != "nan" else None,
                    source="FERC",
                    news_links="[]",
                )
                db.add(project)
            count += 1
        except Exception as e:
            logger.debug("PJM row error: %s", e)
            continue

    db.commit()
    logger.info("PJM queue: upserted %d gas projects", count)
    return count


# ---------------------------------------------------------------------------
# News scraping for OEM / EPC
# ---------------------------------------------------------------------------

def scrape_news_for_oem_epc(db: Session) -> int:
    """For projects missing OEM or EPC, search Google News RSS and parse articles."""
    # Process all projects missing OEM or EPC, largest first (most newsworthy)
    projects = (
        db.query(Project)
        .filter((Project.oem == None) | (Project.epc == None))  # noqa: E711
        .order_by(Project.capacity_mw.desc())
        .limit(200)
        .all()
    )

    changed_ids: list[int] = []
    for project in projects:
        time.sleep(1)  # be polite
        oem, epc, cost, cost_url, links = _search_news(project.name, project.state, project.capacity_mw)
        changed = False
        if oem and not project.oem:
            project.oem = oem
            changed = True
        if epc and not project.epc:
            project.epc = epc
            changed = True
        if cost and not project.cost_per_kw:
            project.cost_per_kw = cost
            project.cost_source_url = cost_url
            changed = True
        if links:
            try:
                existing_links = json.loads(project.news_links or "[]")
                merged = list({*existing_links, *links})
                project.news_links = json.dumps(merged[:20])
                changed = True
            except Exception:
                project.news_links = json.dumps(links[:20])
                changed = True
        if changed:
            project.last_updated = datetime.utcnow()
            changed_ids.append(project.id)

    db.commit()
    logger.info("News scrape: updated OEM/EPC for %d projects", len(changed_ids))
    return len(changed_ids)


def _fetch_article_text(url: str, max_bytes: int = 8000) -> str:
    """Follow a URL and extract visible text from the HTML body."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=10, stream=True)
        raw = b""
        for chunk in r.iter_content(max_bytes):
            raw += chunk
            if len(raw) >= max_bytes:
                break
        r.close()
        # Strip tags, decode
        # Try utf-8, fall back to latin-1 for legacy pages
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                decoded = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            decoded = raw.decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", " ", decoded)
        text = re.sub(r"\s+", " ", text)
        return text[:4000]
    except Exception:
        return ""


def _name_variants(plant_name: str) -> list:
    """Generate shorter search-friendly name variants (drop trailing suffixes)."""
    variants = [plant_name]
    # Drop common EIA suffixes that news omits: "Energy Station", "Power Plant", etc.
    for suffix in [
        " Energy Station", " Power Station", " Electric Generating Station",
        " Generating Station", " Power Plant", " Energy Center", " Energy Project",
        " Combined Cycle Facility", " Combined Cycle", " Gas Plant",
    ]:
        if plant_name.endswith(suffix):
            short = plant_name[: -len(suffix)].strip()
            if short and short not in variants:
                variants.append(short)
    return variants


def _extract_cost_per_kw(text: str, capacity_mw: Optional[float]) -> Optional[float]:
    """Try to extract a $/kW cost figure from article text.

    Looks for:
      1. Direct $/kW mentions (e.g. "$1,200 per kW")
      2. Total project cost ($X million/billion) combined with known capacity
    Returns $/kW rounded to nearest dollar, or None.
    """
    # 1. Direct $/kW
    m = COST_PER_KW_RE.search(text)
    if m:
        val = float(m.group(1).replace(",", ""))
        if 200 <= val <= 5000:  # sanity-check plausible range
            return float(round(val))

    # 2. Total cost ÷ capacity
    if capacity_mw and capacity_mw > 0:
        for m in TOTAL_COST_RE.finditer(text):
            amt = float(m.group(1).replace(",", ""))
            unit = m.group(2).lower()
            if unit == "billion":
                amt *= 1000  # → millions
            cost_per_kw = (amt * 1_000_000) / (capacity_mw * 1000)
            if 200 <= cost_per_kw <= 5000:
                return float(round(cost_per_kw))

    return None


def _search_news(plant_name: str, state: str, capacity_mw: Optional[float] = None) -> tuple:
    """Search Google News RSS + fetch article text.

    Returns (oem, epc, cost_per_kw, cost_source_url, [urls]).
    """
    variants = _name_variants(plant_name)
    queries = []
    for v in variants:
        queries += [
            f'"{v}" turbine GE Siemens Mitsubishi power plant',
            f'"{v}" EPC contractor construction power plant',
            f'"{v}" cost million billion power plant',
            f"{v} {state} natural gas power plant",
        ]

    oem_found = None
    epc_found = None
    cost_found: Optional[float] = None
    cost_url: Optional[str] = None
    links = []

    for query in queries:
        rss_url = (
            f"https://news.google.com/rss/search?q={requests.utils.quote(query)}"
            f"&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            feed = feedparser.parse(rss_url)
        except Exception:
            continue

        for entry in feed.entries[:4]:
            url = entry.get("link", "")
            title = entry.get("title", "")
            links.append(url)

            # Fetch actual article text — much richer than RSS summary
            article_text = title + " " + _fetch_article_text(url)

            # OEM detection in full text
            if not oem_found:
                for oem in KNOWN_OEMS:
                    if oem.lower() in article_text.lower():
                        oem_found = oem
                        logger.info("OEM match '%s' for %s", oem, plant_name)
                        break

            # EPC detection: only match known EPC names near EPC-related keywords
            if not epc_found:
                epc_contexts = []
                for m in EPC_CONTEXT_PATTERN.finditer(article_text):
                    start = max(0, m.start() - 150)
                    end = min(len(article_text), m.end() + 150)
                    epc_contexts.append(article_text[start:end])
                context_blob = " ".join(epc_contexts)
                if context_blob:
                    for epc in KNOWN_EPCS:
                        if epc.lower() in context_blob.lower():
                            epc_found = epc
                            logger.info("EPC match '%s' for %s", epc, plant_name)
                            break

            # Cost extraction
            if not cost_found:
                cost_found = _extract_cost_per_kw(article_text, capacity_mw)
                if cost_found:
                    cost_url = url
                    logger.info("Cost match $%.0f/kW for %s (source: %s)", cost_found, plant_name, url)

            if oem_found and epc_found and cost_found:
                break
            time.sleep(0.5)

        if oem_found and epc_found and cost_found:
            break

    return oem_found, epc_found, cost_found, cost_url, list(dict.fromkeys(links))  # deduplicated


# ---------------------------------------------------------------------------
# Combined entrypoint
# ---------------------------------------------------------------------------

def run_full_scrape(db: Session) -> dict:
    eia_count = scrape_eia(db)
    ferc_count = scrape_ferc_queues(db)
    return {"eia": eia_count, "ferc": ferc_count}


def run_news_scrape(db: Session) -> dict:
    updated = scrape_news_for_oem_epc(db)
    return {"news_updated": updated}
