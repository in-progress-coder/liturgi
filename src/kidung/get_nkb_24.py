#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

SOURCE_URL = "https://www.gkiharapanindah.org/nyanyian-jemaat/nyanyikanlah-kidung-baru/nkb-024-tuhan-kasihanilah-kami/"

DB_PATH = Path("hymns.sqlite3")
LOG_PATH = Path("scrape_one.log")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20.0

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hymns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buku TEXT NOT NULL,
    no_lagu INTEGER NOT NULL,
    judul_text TEXT NOT NULL,
    info TEXT,
    tune TEXT,
    beat TEXT,
    lirik_json TEXT NOT NULL,
    source_url TEXT NOT NULL,
    lyrics_source_url TEXT,
    fetched_at TEXT NOT NULL,
    warnings TEXT,
    UNIQUE (buku, no_lagu) ON CONFLICT REPLACE
);
"""

PUNCT_MAP = {
    "\u2018": "'",  # ‘
    "\u2019": "'",  # ’
    "\u201C": '"',  # “
    "\u201D": '"',  # ”
    "\u2013": "-",  # –
    "\u2014": "-",  # —
    "\u00A0": " ",  # nbsp
}

def setup_logging(verbose: bool = True) -> None:
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH, encoding="utf-8")],
    )

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,id;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    })
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(408, 425, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    return s

def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"
    return r.text

def normalize(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    for k, v in PUNCT_MAP.items():
        s = s.replace(k, v)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = "\n".join(ln.strip() for ln in s.split("\n"))
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# Title patterns:
# - Standard:  "NKB 024 – Title"
# - Non-std:   "NKB 024, Title"
TITLE_RE_STRICT = re.compile(r"^(KJ|PKJ|NKB)\s+0*(\d+)\s*(?:[-—–]|,)\s+(.*)$", re.IGNORECASE)
TITLE_RE_FUZZY  = re.compile(r"(KJ|PKJ|NKB)\s+0*(\d+)\b", re.IGNORECASE)

def split_title(raw_title: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    t = normalize(raw_title) or ""
    m = TITLE_RE_STRICT.match(t)
    if m:
        buku = m.group(1).upper()
        no_lagu = int(m.group(2))
        judul_text = m.group(3).strip() or None
        return buku, no_lagu, judul_text
    # Fallback: find buku & number anywhere, judul_text is the remainder after the first separator
    m2 = TITLE_RE_FUZZY.search(t)
    if m2:
        buku = m2.group(1).upper()
        no_lagu = int(m2.group(2))
        # Try to split after the first dash/comma if present, else everything after match
        rest = t[m2.end():].lstrip()
        # Accept separators '–', '—', '-' or ','
        rest = re.sub(r"^[-—–,]\s*", "", rest)
        judul_text = rest if rest else None
        return buku, no_lagu, judul_text
    # Last resort: return entire title as judul_text
    return None, None, t if t else None

def extract_metadata_from_source(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.entry") or soup.select_one("article.post") or soup.select_one("article")
    if not article:
        raise ValueError("Article element not found.")

    title_el = article.select_one("h1.entry-title") or article.select_one("h1")
    raw_title = title_el.get_text(strip=True) if title_el else ""
    buku, no_lagu, judul_text = split_title(raw_title)

    content = article.select_one("div.entry-content") or article
    info = tune = beat = None
    if content:
        p_texts = [normalize(p.get_text(separator="\n", strip=True)) for p in content.select("p")]
        p_texts = [p for p in p_texts if p]
        if p_texts:
            info = p_texts[0]
        for line in "\n".join(p_texts).split("\n"):
            l = line.strip().lower()
            if tune is None and l.startswith("do ="):
                tune = line.strip()
            if beat is None and "ketuk" in l:
                beat = line.strip()
            if tune and beat:
                break

    return {
        "buku": buku,
        "no_lagu": no_lagu,
        "judul_text": judul_text,
        "info": info,
        "tune": tune,
        "beat": beat,
        "raw_title": raw_title,
    }

# ==== alkitab.app (lyrics) — same structure as your KPPK/KPRI ====
def _text_or_none(parent: BeautifulSoup, selector: str) -> Optional[str]:
    el = parent.select_one(selector)
    return normalize(el.get_text(separator=" ", strip=True)) if el else None

def parse_alkitab_lirik_blocks(lirik_div: BeautifulSoup) -> Dict[str, object]:
    lirik_no_raw = _text_or_none(lirik_div, "div.lirik_no")
    lirik_no = None
    if lirik_no_raw:
        m = re.search(r"(\d+)\s*$", lirik_no_raw)
        lirik_no = m.group(1) if m else lirik_no_raw

    parts = []
    for b in lirik_div.select("div.bait"):
        classes = b.get("class", [])
        is_reff = "reff" in classes
        lines = [
            normalize(x.get_text(separator=" ", strip=True))
            for x in b.select(".baris")
        ]
        lines = [x for x in lines if x]
        text = "\n".join(lines) if lines else None

        if is_reff:
            parts.append({"type": "reff", "no": None, "text": text})
        else:
            no_raw = _text_or_none(b, ".bait-no")
            no = None
            if no_raw:
                m2 = re.search(r"\d+", no_raw)
                no = m2.group(0) if m2 else no_raw
            parts.append({"type": "bait", "no": no, "text": text})
    return {"lirik_no": lirik_no, "parts": parts}

def parse_alkitab_page(html: str) -> Optional[List[Dict[str, object]]]:
    soup = BeautifulSoup(html, "html.parser")
    lagu = soup.select_one("div.lagu")
    if not lagu:
        return None
    return [parse_alkitab_lirik_blocks(l) for l in lagu.select("div.lirik")]

def page_says_no_such_song(html: str) -> bool:
    return "no such song" in (html or "").lower()

def build_alkitab_url(buku: str, no_lagu: int) -> str:
    return f"https://alkitab.app/{buku.upper()}/{int(no_lagu)}"

def fetch_lyrics_from_alkitab(session: requests.Session, buku: str, no_lagu: int) -> Tuple[Optional[List[Dict[str, object]]], str, List[str]]:
    warns: List[str] = []
    base = build_alkitab_url(buku, no_lagu)
    # 1) Try base number
    try:
        h = fetch_html(session, base)
        if page_says_no_such_song(h):
            warns.append("alkitab.app says 'no such song' (base); will try 'A'.")
            raise ValueError("no such song")
        lst = parse_alkitab_page(h)
        if not lst:
            warns.append("no .lagu/.lirik found (base)")
        elif all((not blk.get("parts") for blk in lst)):
            warns.append("no parts found (base)")
        return lst, base, warns
    except requests.HTTPError as e:
        code = e.response.status_code if e.response else "?"
        if str(code) == "404":
            warns.append("HTTP 404 (base); will try 'A'.")
        else:
            warns.append(f"HTTP {code} (base)")
    except Exception as e:
        if "no such song" not in str(e).lower():
            warns.append(f"parse error (base): {e}")
    # 2) Try A-suffix
    aurl = f"{base}A"
    try:
        h2 = fetch_html(session, aurl)
        if page_says_no_such_song(h2):
            warns.append("alkitab.app says 'no such song' (A).")
            return None, aurl, warns
        lst2 = parse_alkitab_page(h2)
        if not lst2:
            warns.append("no .lagu/.lirik found (A)")
        elif all((not blk.get("parts") for blk in lst2)):
            warns.append("no parts found (A)")
        warns.append("used 'A' suffix fallback.")
        return lst2, aurl, warns
    except Exception as e:
        warns.append(f"A-suffix fetch/parse error: {e}")
    return None, aurl, warns

# ==== DB ====
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(SCHEMA_SQL)
    # Ensure columns exist
    cols = {row[1] for row in conn.execute("PRAGMA table_info(hymns)").fetchall()}
    if "lirik_json" not in cols:
        conn.execute("ALTER TABLE hymns ADD COLUMN lirik_json TEXT;")
    if "lyrics_source_url" not in cols:
        conn.execute("ALTER TABLE hymns ADD COLUMN lyrics_source_url TEXT;")
    conn.commit()
    return conn

def upsert_hymn(conn: sqlite3.Connection, row: Dict) -> None:
    sql = """
    INSERT INTO hymns (buku, no_lagu, judul_text, info, tune, beat, lirik_json,
                       source_url, lyrics_source_url, fetched_at, warnings)
    VALUES (:buku, :no_lagu, :judul_text, :info, :tune, :beat, :lirik_json,
            :source_url, :lyrics_source_url, :fetched_at, :warnings)
    ON CONFLICT(buku, no_lagu) DO UPDATE SET
        judul_text=excluded.judul_text,
        info=excluded.info,
        tune=excluded.tune,
        beat=excluded.beat,
        lirik_json=excluded.lirik_json,
        source_url=excluded.source_url,
        lyrics_source_url=excluded.lyrics_source_url,
        fetched_at=excluded.fetched_at,
        warnings=excluded.warnings;
    """
    conn.execute(sql, row)
    conn.commit()

# ==== Validation ====
def validate(meta: Dict, lirik: Optional[List[Dict[str, object]]], url: str) -> List[str]:
    msgs: List[str] = []
    if not meta.get("buku"):
        msgs.append("Missing buku")
    if meta.get("no_lagu") is None:
        msgs.append("Missing no_lagu")
    if not meta.get("judul_text"):
        msgs.append("Missing judul_text")
    if not meta.get("info"):
        msgs.append("Missing info")
    if not lirik or all((not blk.get("parts") for blk in lirik)):
        msgs.append("No lirik found")
    if msgs:
        logging.warning("Key fields issue for %s: %s", url, "; ".join(msgs))
    return msgs

def main():
    setup_logging(verbose=True)
    session = build_session()

    # 1) Fetch source page (GKIHI) and parse metadata (handles comma-style title)
    logging.info("Fetching source page")
    html = fetch_html(session, SOURCE_URL)
    meta = extract_metadata_from_source(html)

    # 2) Fetch lyrics from alkitab.app using buku & no_lagu (with 'A' fallback)
    lirik_list: Optional[List[Dict[str, object]]] = None
    lyrics_url: Optional[str] = None
    lyric_warns: List[str] = []

    if meta.get("buku") and meta.get("no_lagu") is not None:
        lirik_list, lyrics_url, lyric_warns = fetch_lyrics_from_alkitab(
            session, meta["buku"], int(meta["no_lagu"])
        )
    else:
        lyric_warns.append("Cannot fetch alkitab.app lyrics: buku/no_lagu not available.")

    # 3) Validate and upsert
    warns = validate(meta, lirik_list, SOURCE_URL) + lyric_warns
    row = {
        "buku": meta.get("buku"),
        "no_lagu": meta.get("no_lagu"),
        "judul_text": meta.get("judul_text"),
        "info": meta.get("info"),
        "tune": meta.get("tune"),
        "beat": meta.get("beat"),
        "lirik_json": json.dumps(lirik_list or [], ensure_ascii=False),
        "source_url": SOURCE_URL,
        "lyrics_source_url": lyrics_url,
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "warnings": "; ".join(warns) if warns else None,
    }

    conn = init_db()
    upsert_hymn(conn, row)
    logging.info("Saved: %s %s — %s", row["buku"], row["no_lagu"], row["judul_text"])
    if row["warnings"]:
        logging.warning("Warnings: %s", row["warnings"])

    # Optional: print a brief summary to stdout
    print(json.dumps({
        "buku": row["buku"],
        "no_lagu": row["no_lagu"],
        "judul_text": row["judul_text"],
        "lyrics_source_url": row["lyrics_source_url"],
        "has_lirik": bool(lirik_list and any(blk.get("parts") for blk in lirik_list))
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
