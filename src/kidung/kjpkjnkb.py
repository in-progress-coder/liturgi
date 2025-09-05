#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import random
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

# ----------------------------
# Configuration
# ----------------------------
DEFAULT_DB = "hymns.sqlite3"
DEFAULT_FILES = {
    "KJ": "kj_links.txt",
    "PKJ": "pkj_links.txt",
    "NKB": "nkb_links.txt",
}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20.0

# ----------------------------
# Logging
# ----------------------------
def setup_logging(log_path: Path, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    logging.debug("Logging initialized: %s", log_path)

# ----------------------------
# HTTP session (retries)
# ----------------------------
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
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch_html(session: requests.Session, url: str, timeout: float = REQUEST_TIMEOUT) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"
    return r.text

# ----------------------------
# SQLite schema & upsert
# ----------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hymns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buku TEXT NOT NULL,
    no_lagu INTEGER NOT NULL,
    judul_text TEXT NOT NULL,
    info TEXT,
    tune TEXT,
    beat TEXT,
    lirik_json TEXT NOT NULL,   -- list of lirik blocks (below)
    source_url TEXT NOT NULL,
    lyrics_source_url TEXT,
    fetched_at TEXT NOT NULL,
    warnings TEXT,
    UNIQUE (buku, no_lagu) ON CONFLICT REPLACE
);
"""

def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(SCHEMA_SQL)
    # Defensive migration if table existed differently
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

# ----------------------------
# Text normalization & title split
# ----------------------------
PUNCT_MAP = {
    "\u2018": "'",  # ‘
    "\u2019": "'",  # ’
    "\u201C": '"',  # “
    "\u201D": '"',  # ”
    "\u2013": "-",  # –
    "\u2014": "-",  # —
    "\u00A0": " ",  # nbsp
}

def normalize_preserve_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    for k, v in PUNCT_MAP.items():
        s = s.replace(k, v)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = "\n".join(ln.strip() for ln in s.split("\n"))
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

TITLE_RE = re.compile(r"^(KJ|PKJ|NKB)\s+(\d+)\s+[-–]\s+(.*)$", re.IGNORECASE)

def split_title(raw_title: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    t = normalize_preserve_newlines(raw_title) or ""
    m = TITLE_RE.match(t)
    if not m:
        return None, None, t if t else None
    buku = m.group(1).upper()
    no_lagu = int(m.group(2))
    judul_text = m.group(3).strip() or None
    return buku, no_lagu, judul_text

# ----------------------------
# Metadata parser (source page)
# ----------------------------
def extract_metadata_from_source(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("article.entry") or soup.select_one("article.post") or soup.select_one("article")
    if not article:
        raise ValueError("Article element not found with expected selectors.")

    title_el = article.select_one("h1.entry-title") or article.select_one("h1")
    raw_title = title_el.get_text(strip=True) if title_el else ""
    buku, no_lagu, judul_text = split_title(raw_title)

    content = article.select_one("div.entry-content") or article
    info = tune = beat = None
    if content:
        p_texts = [
            normalize_preserve_newlines(p.get_text(separator="\n", strip=True))
            for p in content.select("p")
        ]
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

# ----------------------------
# Lyrics parser (alkitab.app) — SAME STRUCTURE AS YOUR KPPK/KPRI SCRIPT
# ----------------------------
def _text_or_none(parent: BeautifulSoup, selector: str) -> Optional[str]:
    el = parent.select_one(selector)
    return normalize_preserve_newlines(el.get_text(separator=" ", strip=True)) if el else None

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
            normalize_preserve_newlines(x.get_text(separator=" ", strip=True))
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
    lirik_list = [parse_alkitab_lirik_blocks(l) for l in lagu.select("div.lirik")]
    return lirik_list

def build_alkitab_url(buku: str, no_lagu: int) -> str:
    return f"https://alkitab.app/{buku.upper()}/{int(no_lagu)}"

def page_says_no_such_song(html: str) -> bool:
    return "no such song" in (html or "").lower()

def fetch_lyrics_from_alkitab(session: requests.Session, buku: str, no_lagu: int) -> Tuple[Optional[List[Dict[str, object]]], str, List[str]]:
    """
    Try /{no_lagu}, and if the page says 'no such song' (or 404), retry /{no_lagu}A.
    Returns (lirik_list or None, lyrics_url_used, warnings)
    """
    warns: List[str] = []

    # 1) first attempt: base number
    url_base = build_alkitab_url(buku, no_lagu)
    try:
        html = fetch_html(session, url_base)
        if page_says_no_such_song(html):
            warns.append("alkitab.app responded 'no such song' (base); retry with 'A' suffix.")
            raise ValueError("no such song (base)")
        lirik_list = parse_alkitab_page(html)
        if not lirik_list:
            # Not strictly 'no such song', but keep result and warn
            warns.append("no .lagu/.lirik found at alkitab.app (base)")
        else:
            if all((not blk.get("parts") for blk in lirik_list)):
                warns.append("no parts (bait/reff) found at alkitab.app (base)")
        return lirik_list, url_base, warns
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        if str(code) == "404":
            warns.append("HTTP 404 at alkitab.app (base); retry with 'A' suffix.")
        else:
            warns.append(f"alkitab.app fetch failed (base): HTTP {code}")
        # fall through to try 'A'
    except Exception as e:
        msg = str(e).lower()
        if "no such song" in msg:
            # proceed to 'A'
            pass
        else:
            # unexpected error, proceed to 'A' attempt but record
            warns.append(f"alkitab.app parse error (base): {e}")

    # 2) second attempt: number + 'A'
    url_a = f"{url_base}A"
    try:
        html_a = fetch_html(session, url_a)
        if page_says_no_such_song(html_a):
            warns.append("alkitab.app responded 'no such song' (A variant).")
            return None, url_a, warns
        lirik_list_a = parse_alkitab_page(html_a)
        if not lirik_list_a:
            warns.append("no .lagu/.lirik found at alkitab.app (A variant).")
        else:
            if all((not blk.get("parts") for blk in lirik_list_a)):
                warns.append("no parts (bait/reff) found at alkitab.app (A variant).")
        warns.append("used 'A' suffix fallback.")
        return lirik_list_a, url_a, warns
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        warns.append(f"alkitab.app fetch failed (A variant): HTTP {code}")
    except Exception as e:
        warns.append(f"alkitab.app parse error (A variant): {e}")

    return None, url_a, warns

# ----------------------------
# Validation & transformation
# ----------------------------
def enforce_required_fields(meta: Dict, lirik_list: Optional[List[Dict[str, object]]], url: str) -> List[str]:
    msgs: List[str] = []
    if not meta.get("buku"):
        msgs.append("Missing buku")
    if meta.get("no_lagu") is None:
        msgs.append("Missing no_lagu")
    if not meta.get("judul_text"):
        msgs.append("Missing judul_text")
    if not meta.get("info"):
        msgs.append("Missing info")
    if not lirik_list:
        msgs.append("No lirik found")
    elif all((not blk.get("parts") for blk in lirik_list)):
        msgs.append("No lirik parts found")
    if msgs:
        logging.warning("Key fields issue for %s: %s", url, "; ".join(msgs))
    return msgs

def to_db_row(meta: Dict, source_url: str, lirik_list: Optional[List[Dict[str, object]]], lyrics_source_url: Optional[str], extra_warnings: List[str]) -> Dict:
    lirik_json = json.dumps(lirik_list or [], ensure_ascii=False)
    return {
        "buku": meta.get("buku"),
        "no_lagu": meta.get("no_lagu"),
        "judul_text": meta.get("judul_text"),
        "info": meta.get("info"),
        "tune": meta.get("tune"),
        "beat": meta.get("beat"),
        "lirik_json": lirik_json,
        "source_url": source_url,
        "lyrics_source_url": lyrics_source_url,
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "warnings": "; ".join(extra_warnings) if extra_warnings else None,
    }

# ----------------------------
# I/O helpers
# ----------------------------
def read_links(file_path: Path) -> List[str]:
    if not file_path.exists():
        logging.info("Link file not found: %s", file_path)
        return []
    links: List[str] = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            links.append(s)
    return links

# ----------------------------
# Scrape 1 URL
# ----------------------------
def scrape_one(session: requests.Session, url: str, expected_buku: Optional[str]) -> Dict:
    html = fetch_html(session, url)
    meta = extract_metadata_from_source(html)

    # If buku not parsed, fallback to expected from file name
    if not meta.get("buku") and expected_buku:
        meta["buku"] = expected_buku.upper()
        logging.warning("Buku missing in page, using expected from file: %s", meta["buku"])
    # If mismatch parsed vs expected (if provided), warn (keep parsed)
    if meta.get("buku") and expected_buku and meta["buku"].upper() != expected_buku.upper():
        logging.warning("Buku mismatch (parsed=%s, expected=%s) for %s", meta["buku"], expected_buku, url)

    lirik_list: Optional[List[Dict[str, object]]] = None
    lyrics_source_url: Optional[str] = None
    lyric_warns: List[str] = []

    if meta.get("buku") and meta.get("no_lagu") is not None:
        lirik_list, lyrics_source_url, lyric_warns = fetch_lyrics_from_alkitab(
            session, meta["buku"], int(meta["no_lagu"])
        )
    else:
        lyric_warns.append("Cannot fetch alkitab.app lyrics: buku/no_lagu not available.")

    key_warns = enforce_required_fields(meta, lirik_list, url)
    all_warns = key_warns + lyric_warns
    return to_db_row(meta, url, lirik_list, lyrics_source_url, all_warns)

# ----------------------------
# Process one link file
# ----------------------------
def process_file(session: requests.Session, conn: sqlite3.Connection, hymnal: str, file_path: Path, testing: bool, sleep_s: float) -> Tuple[int, int]:
    links = read_links(file_path)
    if not links:
        logging.info("[%s] No links to process in %s", hymnal, file_path)
        return (0, 0)
    if testing:
        k = min(5, len(links))
        links = random.sample(links, k)
        logging.info("[%s] TESTING mode: sampling %d links", hymnal, k)
    else:
        logging.info("[%s] Processing %d links", hymnal, len(links))

    ok = err = 0
    for i, url in enumerate(links, start=1):
        logging.info("[%s] (%d/%d) %s", hymnal, i, len(links), url)
        try:
            row = scrape_one(session, url, expected_buku=hymnal)
            upsert_hymn(conn, row)
            if row.get("warnings"):
                logging.warning("[%s] Saved with warnings: %s", hymnal, row["warnings"])
            else:
                logging.debug("[%s] Saved OK", hymnal)
            ok += 1
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            logging.error("[%s] HTTP error %s for %s", hymnal, code, url)
            err += 1
        except Exception as e:
            logging.exception("[%s] Failed parsing %s: %s", hymnal, url, e)
            err += 1
        time.sleep(sleep_s)

    logging.info("[%s] Done. OK=%d, ERR=%d", hymnal, ok, err)
    return ok, err

# ----------------------------
# Entrypoint
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Scrape hymn metadata from source pages and lyrics from alkitab.app (with 'A' fallback) into SQLite."
    )
    parser.add_argument("--db", type=Path, default=Path(DEFAULT_DB), help="SQLite DB path")
    parser.add_argument("--kj", type=Path, default=Path(DEFAULT_FILES["KJ"]), help="Path to kj_links.txt")
    parser.add_argument("--pkj", type=Path, default=Path(DEFAULT_FILES["PKJ"]), help="Path to pkj_links.txt")
    parser.add_argument("--nkb", type=Path, default=Path(DEFAULT_FILES["NKB"]), help="Path to nkb_links.txt")
    parser.add_argument("--testing", action="store_true", help="5 random links per file")
    parser.add_argument("--sleep", type=float, default=0.8, help="Sleep seconds between requests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    log_path = Path("scrape_hymns.log")
    setup_logging(log_path, verbose=args.verbose)

    conn = init_db(args.db)
    session = build_session()

    totals = {"OK": 0, "ERR": 0}
    for hymnal, fp in [("KJ", args.kj), ("PKJ", args.pkj), ("NKB", args.nkb)]:
        ok, err = process_file(session, conn, hymnal, fp, testing=args.testing, sleep_s=args.sleep)
        totals["OK"] += ok
        totals["ERR"] += err

    logging.info("ALL DONE. TOTAL OK=%d, ERR=%d. DB=%s, LOG=%s",
                 totals["OK"], totals["ERR"], args.db, log_path)

if __name__ == "__main__":
    main()
