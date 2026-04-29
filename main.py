import os
import traceback
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Löwen Frankfurt News")


# =====================================================
# DB
# =====================================================

def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )


# =====================================================
# BASIC ROUTES
# =====================================================

@app.get("/")
def root():
    return {"status": "running ✅", "app": "Löwen Frankfurt News"}

@app.get("/health")
def health():
    return {"ok": True}


# =====================================================
# SETUP (passt zu deinem Schema: sources.feed_url NOT NULL)
# =====================================================

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT NOT NULL UNIQUE
        );
    """)

    # content ist bei dir offenbar NOT NULL -> Default '' schützt uns zusätzlich.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            source_url TEXT UNIQUE,
            source_id INTEGER,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"setup": "ok ✅"}


# =====================================================
# NEWS API
# =====================================================

@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            news.id,
            news.title,
            sources.name AS source,
            news.source_url AS link,
            COALESCE(
                news.published_at,
                news.created_at
            ) AS date
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        ORDER BY
            COALESCE(news.published_at, news.created_at) DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/news/loewen")
def list_loewen_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            news.id,
            news.title,
            sources.name AS source,
            news.source_url AS link,
            COALESCE(
                news.published_at,
                news.created_at
            ) AS date
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        WHERE news.category = 'loewen_frankfurt'
        ORDER BY
            COALESCE(news.published_at, news.created_at) DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# =====================================================
# IMPORT (RSS + HTML) – robust, 500-sicher
# =====================================================

@app.get("/rss/import")
def rss_import():
    inserted = 0
    errors = []

    # Phase 1: Team-spezifisch (Kategorie erzwingen)
    TEAM_FEEDS_RSS = [
        ("sport.de – Löwen Frankfurt", "https://www.sport.de/rss/news/te2940/loewen-frankfurt/"),
        # EishockeyNews ist eine Seite; feedparser kommt oft klar, sonst gibt es keinen Crash, nur ggf. 0 entries
        ("Eishockey NEWS – Löwen Frankfurt", "https://www.eishockeynews.de/del/loewen-frankfurt"),
    ]

    # Phase 2: Szene/Blog (RSS + Filter)
    BLOG_FEEDS = [
        ("Eisblog", "https://eisblog.media/feed/"),
    ]
    BLOG_KEYWORDS = ["löwen", "loewen", "frankfurt"]

    # Phase 3: Regionale Medien (HTML Headlines)
    SCRAPE_SOURCES = [
        ("FNP", "https://www.fnp.de/sport/loewen-frankfurt/"),
        ("OP-Online", "https://www.op-online.de/sport/loewen-frankfurt/"),
    ]

    # Phase 4: OFFIZIELL + PENNY DEL (HTML Headlines)
    OFFICIAL_HTML = [
        ("Löwen Frankfurt (offiziell) – Aktuelles", "https://www.loewen-frankfurt.de/saison/aktuelles/"),
        ("PENNY DEL – News", "https://www.penny-del.org/news/"),
    ]

    headers = {"User-Agent": "LoewenNewsBot/1.0"}

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # ---------------------------
        # Helper: Source upsert + id holen
        # ---------------------------
        def ensure_source(name: str, feed_url: str):
            cur.execute(
                "INSERT INTO sources (name, feed_url) VALUES (%s,%s) ON CONFLICT (feed_url) DO NOTHING",
                (name, feed_url),
            )
            cur.execute("SELECT id FROM sources WHERE feed_url=%s", (feed_url,))
            row = cur.fetchone()
            return row["id"] if row else None

        # =================================================
        # PHASE 1 – TEAM (RSS/Feedparser)
        # =================================================
        for name, feed_url in TEAM_FEEDS_RSS:
            try:
                sid = ensure_source(name, feed_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE1:{name}] could not get source_id")
                    continue

                feed = feedparser.parse(feed_url)
                entries = getattr(feed, "entries", []) or []

                for e in entries:
                    title = (e.get("title") or "").strip()
                    link = e.get("link")
                    summary = (e.get("summary") or e.get("description") or "").strip()

                    if not title or not link:
                        continue

                    content = summary if summary else title  # niemals NULL
                    cur.execute("""
                        INSERT INTO news (title, content, source_url, source_id, category)
                        VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                        ON CONFLICT (source_url)
                        DO UPDATE SET category='loewen_frankfurt';
                    """, (title, content, link, sid))

                    inserted += cur.rowcount

                conn.commit()

            except Exception as e:
                conn.rollback()
                errors.append(f"[PHASE1:{name}] {repr(e)}")
                continue

        # =================================================
        # PHASE 2 – BLOG (RSS + Keyword Filter)
        # =================================================
        for name, feed_url in BLOG_FEEDS:
            try:
                sid = ensure_source(name, feed_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE2:{name}] could not get source_id")
                    continue

                feed = feedparser.parse(feed_url)
                entries = getattr(feed, "entries", []) or []

                for e in entries:
                    title = (e.get("title") or "").strip()
                    link = e.get("link")
                    summary = (e.get("summary") or e.get("description") or "").strip()

                    if not title or not link:
                        continue

                    text = f"{title} {summary}".lower()
                    if not any(k in text for k in BLOG_KEYWORDS):
                        continue

                    content = summary if summary else title
                    cur.execute("""
                        INSERT INTO news (title, content, source_url, source_id, category)
                        VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                        ON CONFLICT (source_url)
                        DO UPDATE SET category='loewen_frankfurt';
                    """, (title, content, link, sid))

                    inserted += cur.rowcount

                conn.commit()

            except Exception as e:
                conn.rollback()
                errors.append(f"[PHASE2:{name}] {repr(e)}")
                continue

        # =================================================
        # PHASE 3 – REGIONALE MEDIEN (HTML Headlines)
        # =================================================
        for name, page_url in SCRAPE_SOURCES:
            try:
                sid = ensure_source(name, page_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE3:{name}] could not get source_id")
                    continue

                resp = requests.get(page_url, headers=headers, timeout=8)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.select("a[href]")[:60]:
                    title = a.get_text(strip=True)
                    href = a.get("href")

                    if not title:
                        continue
                    if "löwen" not in title.lower():
                        continue

                    link = urljoin(page_url, href)
                    content = title  # niemals NULL

                    cur.execute("""
                        INSERT INTO news (title, content, source_url, source_id, category)
                        VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                        ON CONFLICT (source_url)
                        DO UPDATE SET category='loewen_frankfurt';
                    """, (title, content, link, sid))

                    inserted += cur.rowcount

                conn.commit()

            except Exception as e:
                conn.rollback()
                errors.append(f"[PHASE3:{name}] {repr(e)}")
                continue

        # =================================================
        # PHASE 4 – OFFIZIELL + PENNY DEL (HTML Headlines)
        # =================================================
        for name, page_url in OFFICIAL_HTML:
            try:
                sid = ensure_source(name, page_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE4:{name}] could not get source_id")
                    continue

                resp = requests.get(page_url, headers=headers, timeout=8)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                if "loewen-frankfurt.de" in page_url:
                    # Offizielle Aktuelles-Links sind typischerweise /saison/aktuelles/details/...
                    anchors = soup.select("a[href^='/saison/aktuelles/details/']")
                    base = "https://www.loewen-frankfurt.de"
                    for a in anchors[:80]:
                        title = a.get_text(strip=True)
                        href = a.get("href")
                        if not title or not href:
                            continue
                        link = urljoin(base, href)
                        content = title
                        cur.execute("""
                            INSERT INTO news (title, content, source_url, source_id, category)
                            VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                            ON CONFLICT (source_url)
                            DO UPDATE SET category='loewen_frankfurt';
                        """, (title, content, link, sid))
                        inserted += cur.rowcount

                else:
                    # PENNY DEL News: viele Liga-News -> filter auf Frankfurt/Löwen im Titel
                    anchors = soup.select("a[href^='/news/']")
                    base = "https://www.penny-del.org"
                    for a in anchors[:120]:
                        title = a.get_text(strip=True)
                        href = a.get("href")
                        if not title or not href:
                            continue
                        t = title.lower()
                        if ("frankfurt" not in t) and ("löwen" not in t) and ("loewen" not in t):
                            continue
                        link = urljoin(base, href)
                        content = title
                        cur.execute("""
                            INSERT INTO news (title, content, source_url, source_id, category)
                            VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                            ON CONFLICT (source_url)
                            DO UPDATE SET category='loewen_frankfurt';
                        """, (title, content, link, sid))
                        inserted += cur.rowcount

                conn.commit()

            except Exception as e:
                conn.rollback()
                errors.append(f"[PHASE4:{name}] {repr(e)}")
                continue

        cur.close()
        conn.close()

    except Exception as fatal:
        # absolutes Sicherheitsnetz: nie 500, sondern Fehler im JSON
        return {
            "status": "fatal_error_caught",
            "error": repr(fatal),
            "trace": traceback.format_exc(),
        }

    return {
        "status": "ok ✅",
        "inserted_or_updated": inserted,
        "errors": errors,
        "sources_added": {
            "loewen_official": "https://www.loewen-frankfurt.de/saison/aktuelles/",
            "penny_del_news": "https://www.penny-del.org/news/",
        }
    }
