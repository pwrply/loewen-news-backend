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

    # sources: feed_url ist die kanonische Spalte und NOT NULL
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT NOT NULL UNIQUE
        );
    """)

    # news: content kann bei dir NOT NULL sein → wir setzen DEFAULT ''
    # (falls Tabelle schon existiert, wird sie dadurch nicht geändert – aber wir schreiben im Code nie NULL)
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
        SELECT news.*, sources.name AS source_name
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        ORDER BY news.created_at DESC;
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
        SELECT news.*, sources.name AS source_name
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        WHERE news.category = 'loewen_frankfurt'
        ORDER BY news.created_at DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# =====================================================
# IMPORT (Phase 1–3) – robust, 500-sicher
# =====================================================

@app.get("/rss/import")
def rss_import():
    inserted = 0
    errors = []

    # Phase 1: Team-spezifisch (Kategorie erzwingen)
    TEAM_FEEDS = [
        ("sport.de – Löwen Frankfurt", "https://www.sport.de/rss/news/te2940/loewen-frankfurt/"),
        # Optional (kann leer sein, falls kein echter RSS): Seite wird trotzdem versucht
        ("Eishockey NEWS – Löwen Frankfurt", "https://www.eishockeynews.de/del/loewen-frankfurt"),
    ]

    # Phase 2: Szene/Blog (RSS + Filter)
    BLOG_FEEDS = [
        ("Eisblog", "https://eisblog.media/feed/"),
    ]
    BLOG_KEYWORDS = ["löwen", "loewen", "frankfurt"]

    # Phase 3: Regionale Medien (HTML Headlines, kein Volltext)
    SCRAPE_SOURCES = [
        ("FNP", "https://www.fnp.de/sport/loewen-frankfurt/"),
        ("OP-Online", "https://www.op-online.de/sport/loewen-frankfurt/"),
    ]
    headers = {"User-Agent": "LoewenNewsBot/1.0"}

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # ---------------------------
        # Helper: Source upsert + id holen
        # ---------------------------
        def ensure_source(name: str, feed_url: str) -> int:
            cur.execute(
                "INSERT INTO sources (name, feed_url) VALUES (%s,%s) ON CONFLICT (feed_url) DO NOTHING",
                (name, feed_url),
            )
            cur.execute("SELECT id FROM sources WHERE feed_url=%s", (feed_url,))
            row = cur.fetchone()
            return row["id"] if row else None

        # =================================================
        # PHASE 1 – TEAM FEEDS
        # =================================================
        for name, feed_url in TEAM_FEEDS:
            try:
                sid = ensure_source(name, feed_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE1:{name}] could not get source_id")
                    continue

                feed = feedparser.parse(feed_url)

                for e in getattr(feed, "entries", []) or []:
                    title = (e.get("title") or "").strip()
                    link = e.get("link")
                    summary = (e.get("summary") or e.get("description") or "").strip()

                    if not title or not link:
                        continue

                    # content darf NIE NULL sein
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
                errors.append(f"[PHASE1:{name}] {repr(e)}")
                continue

        # =================================================
        # PHASE 2 – BLOG FEEDS (Filter)
        # =================================================
        for name, feed_url in BLOG_FEEDS:
            try:
                sid = ensure_source(name, feed_url)
                if not sid:
                    conn.rollback()
                    errors.append(f"[PHASE2:{name}] could not get source_id")
                    continue

                feed = feedparser.parse(feed_url)

                for e in getattr(feed, "entries", []) or []:
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
        # PHASE 3 – HTML SOURCES (Headlines only)
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

                # Wir nehmen nur Headlines/Links und speichern KEINEN Volltext
                for a in soup.select("a[href]")[:40]:
                    title = a.get_text(strip=True)
                    href = a.get("href")

                    if not title:
                        continue

                    # minimale Relevanz: muss "löwen" enthalten
                    if "löwen" not in title.lower():
                        continue

                    link = urljoin(page_url, href)

                    # content darf NIE NULL sein -> fallback ist title
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
                errors.append(f"[PHASE3:{name}] {repr(e)}")
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
    }
