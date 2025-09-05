#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import sqlite3
import logging
import json
import re
from typing import Optional, Dict, List, Tuple
import requests
from bs4 import BeautifulSoup

# -------- Crawl targets --------
TARGETS: List[Tuple[str, int]] = [
    ("KPPK", 425),
    ("KPRI", 171),
]

DB_PATH = "kppk_lagu.sqlite"
LOG_FILE = "scraper.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HymnFinder/1.4; +https://example.local)"
}
RETRY = 3
BACKOFF = 1.5
REQUEST_TIMEOUT = 20
DELAY_BETWEEN_PAGES_SEC = 0.6

# Logging setup: console + file
logger = logging.getLogger("scraper")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# File handler (append mode)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)


# ----------------------------
# HTTP
# ----------------------------
def fetch(url: str, retries: int = RETRY, backoff_sec: float = BACKOFF) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            logger.warning("Fetch failed (%d/%d) %s: %s", attempt, retries, url, e)
            if attempt < retries:
                time.sleep(backoff_sec * attempt)
    return None


# ----------------------------
# Text utilities
# ----------------------------
def normalize_text(s: str) -> Optional[str]:
    if s is None:
        return None
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \u00A0]+", " ", s)
    s = "\n".join(line.strip() for line in s.split("\n")).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s


def text_or_none(parent: BeautifulSoup, selector: str) -> Optional[str]:
    el = parent.select_one(selector)
    return normalize_text(el.get_text(separator=" ", strip=True)) if el else None


def split_judul(full_title: Optional[str]) -> Dict[str, Optional[object]]:
    if not full_title:
        return {"buku": None, "no_lagu": None, "judul_text": None}
    m = re.match(r"^\s*(KPRI|KPPK)\s+(\d+)\s+(.*)\s*$", full_title, flags=re.IGNORECASE)
    if m:
        return {"buku": m.group(1).upper(), "no_lagu": int(m.group(2)), "judul_text": m.group(3).strip()}
    m2 = re.match(r"^\s*(KPRI|KPPK)\s+(\d+)\s*$", full_title, flags=re.IGNORECASE)
    if m2:
        return {"buku": m2.group(1).upper(), "no_lagu": int(m2.group(2)), "judul_text": ""}
    return {"buku": None, "no_lagu": None, "judul_text": full_title.strip()}


# ----------------------------
# Page parsing (your rules)
# ----------------------------
def parse_lirik_blocks(lirik_div: BeautifulSoup) -> Dict[str, object]:
    lirik_no_raw = text_or_none(lirik_div, "div.lirik_no")
    lirik_no = None
    if lirik_no_raw:
        m = re.search(r"(\d+)\s*$", lirik_no_raw)
        lirik_no = m.group(1) if m else lirik_no_raw

    parts = []
    for b in lirik_div.select("div.bait"):
        classes = b.get("class", [])
        is_reff = "reff" in classes

        lines = [normalize_text(x.get_text(separator=" ", strip=True)) for x in b.select(".baris")]
        lines = [x for x in lines if x]
        text = "\n".join(lines) if lines else None

        if is_reff:
            parts.append({"type": "reff", "no": None, "text": text})
        else:
            no_raw = text_or_none(b, ".bait-no")
            no = None
            if no_raw:
                m2 = re.search(r"\d+", no_raw)
                no = m2.group(0) if m2 else no_raw
            parts.append({"type": "bait", "no": no, "text": text})

    return {"lirik_no": lirik_no, "parts": parts}


def parse_page(html: str) -> Optional[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    lagu = soup.select_one("div.lagu")
    if not lagu:
        return None

    full_title = text_or_none(lagu, "div.judul")
    split = split_judul(full_title)

    data = {
        "judul": full_title,
        "buku": split["buku"],
        "no_lagu": split["no_lagu"],
        "judul_text": split["judul_text"],
        "judul_asli": text_or_none(lagu, "div.judul_asli"),
        "pengarang_lirik": text_or_none(lagu, "div.pengarang_lirik"),
        "pengarang_musik": text_or_none(lagu, "div.pengarang_musik"),
        "nadaDasar": text_or_none(lagu, "div.nadaDasar"),
    }

    lirik_list = [parse_lirik_blocks(l) for l in lagu.select("div.lirik")]
    data["lirik_json"] = json.dumps(lirik_list, ensure_ascii=False)

    flat_baits: List[str] = []
    for l in lirik_list:
        for p in l.get("parts", []):
            if p.get("text"):
                flat_baits.append(p["text"])
    data["bait"] = "\n\n".join(flat_baits) if flat_baits else None

    return data


# ----------------------------
# SQLite schema & upsert
# ----------------------------
def column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]
    return col in cols


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lagu (
            buku TEXT NOT NULL,
            page_no INTEGER NOT NULL,
            judul TEXT,
            judul_asli TEXT,
            pengarang_lirik TEXT,
            pengarang_musik TEXT,
            nadaDasar TEXT,
            bait TEXT,
            PRIMARY KEY (buku, page_no)
        );
    """)
    if not column_exists(conn, "lagu", "no_lagu"):
        conn.execute("ALTER TABLE lagu ADD COLUMN no_lagu INTEGER;")
    if not column_exists(conn, "lagu", "judul_text"):
        conn.execute("ALTER TABLE lagu ADD COLUMN judul_text TEXT;")
    if not column_exists(conn, "lagu", "lirik_json"):
        conn.execute("ALTER TABLE lagu ADD COLUMN lirik_json TEXT;")
    conn.commit()


def upsert_record(conn: sqlite3.Connection, buku: str, page_no: int, rec: Dict[str, object]) -> None:
    conn.execute("""
        INSERT INTO lagu (
            buku, page_no, judul, judul_asli, pengarang_lirik, pengarang_musik,
            nadaDasar, bait, no_lagu, judul_text, lirik_json
        )
        VALUES (
            :buku, :page_no, :judul, :judul_asli, :pengarang_lirik, :pengarang_musik,
            :nadaDasar, :bait, :no_lagu, :judul_text, :lirik_json
        )
        ON CONFLICT(buku, page_no) DO UPDATE SET
            judul = excluded.judul,
            judul_asli = excluded.judul_asli,
            pengarang_lirik = excluded.pengarang_lirik,
            pengarang_musik = excluded.pengarang_musik,
            nadaDasar = excluded.nadaDasar,
            bait = excluded.bait,
            no_lagu = excluded.no_lagu,
            judul_text = excluded.judul_text,
            lirik_json = excluded.lirik_json
    """, {"buku": buku, "page_no": page_no, **rec})
    conn.commit()


# ----------------------------
# Crawl loop with validation logs
# ----------------------------
def validate_record(rec: Dict[str, object]) -> List[str]:
    """Return list of warnings about abnormal/missing elements."""
    warnings = []
    if rec.get("no_lagu") is None:
        warnings.append("missing no_lagu")
    if not rec.get("judul_text"):
        warnings.append("missing judul_text")
    if not rec.get("lirik_json") or rec.get("lirik_json") == "[]":
        warnings.append("no lirik found")
    if not rec.get("bait"):
        warnings.append("no bait text found")
    return warnings


def scrape_targets(targets: List[Tuple[str, int]]) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        for buku, max_no in targets:
            for num in range(1, max_no + 1):
                url = f"https://alkitab.app/{buku}/{num}"
                logger.info("Fetching %s", url)
                html = fetch(url)
                if html is None:
                    logger.error("SKIP %s/%d - fetch failed", buku, num)
                    continue
                rec = parse_page(html)
                if rec is None:
                    logger.error("SKIP %s/%d - no <div.lagu> found", buku, num)
                    continue

                # Fallback to URL if parsing didn't detect buku/no_lagu
                if not rec.get("buku"):
                    rec["buku"] = buku
                if not rec.get("no_lagu"):
                    rec["no_lagu"] = num

                upsert_record(conn, buku, num, rec)

                # Validate and log
                warnings = validate_record(rec)
                if warnings:
                    logger.warning("PARSED %s/%d - OK with WARNINGS: %s",
                                   buku, num, ", ".join(warnings))
                else:
                    logger.info("PARSED %s/%d - OK (no_lagu=%s, judul_text='%s')",
                                buku, num, rec.get("no_lagu"), rec.get("judul_text"))

                time.sleep(DELAY_BETWEEN_PAGES_SEC)
    finally:
        conn.close()


if __name__ == "__main__":
    scrape_targets(TARGETS)
    logger.info("Done. Database written to %s, logs in %s", DB_PATH, LOG_FILE)
