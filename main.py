from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime

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
# Setup
# =====================================================

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            source_url TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
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
# API
# =====================================================

@app.get("/news/loewen")
def loewen_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT news.*, sources.name AS source_name
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        WHERE category = 'loewen_frankfurt'
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# =====================================================
# RSS + SCRAPE IMPORT
# =====================================================

@app.get("/rss/import")
def import_all():

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0

    # =================================================
    # PHASE 1 – Team-RSS
    # =================================================
    TEAM_FEEDS = [
        ("sport.de – Löwen Frankfurt",
         "https://www.sport.de/rss/news/te2940/loewen-frankfurt/"),
        ("Eishockey NEWS – Löwen Frankfurt",
         "https://www.eishockeynews.de/del/loewen-frankfurt"),
    ]

    for name, url in TEAM_FEEDS:
        cur.execute(
            "INSERT INTO sources (name, source_url) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (name, url)
        )
        cur.execute("SELECT id FROM sources WHERE source_url=%s", (url,))
        sid = cur.fetchone()["id"]

        feed = feedparser.parse(url)
        for e in feed.entries:
            title = (e.get("title") or "").strip()
            link = e.get("link")
            content = (e.get("summary") or "").strip()

            if not title or not link:
                continue

            cur.execute("""
                INSERT INTO news (title, content, source_url, source_id, category)
                VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                ON CONFLICT (source_url)
                DO UPDATE SET category='loewen_frankfurt'
            """, (title, content, link, sid))
            inserted += cur.rowcount

    # =================================================
    # PHASE 2 – Eisblog (RSS + Filter)
    # =================================================
    BLOG_FEEDS = [("Eisblog", "https://eisblog.media/feed/")]
    KEYWORDS = ["löwen", "loewen", "frankfurt"]

    for name, url in BLOG_FEEDS:
        cur.execute(
            "INSERT INTO sources (name, source_url) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (name, url)
        )
        cur.execute("SELECT id FROM sources WHERE source_url=%s", (url,))
        sid = cur.fetchone()["id"]

        feed = feedparser.parse(url)
        for e in feed.entries:
            title = (e.get("title") or "").strip()
            link = e.get("link")
            content = (e.get("summary") or "").strip()

            text = f"{title} {content}".lower()
            if not any(k in text for k in KEYWORDS):
                continue

            cur.execute("""
                INSERT INTO news (title, content, source_url, source_id, category)
                VALUES (%s,%s,%s,%s,'loewen_frankfurt')
                ON CONFLICT (source_url)
                DO NOTHING
            """, (title, content, link, sid))
            inserted += cur.rowcount

# =================================================
# PHASE 3 – Regionale Medien (HTML Fetch, SAFE)
# =================================================

SCRAPE_SOURCES = [
    ("FNP", "https://www.fnp.de/sport/loewen-frankfurt/"),
    ("OP-Online", "https://www.op-online.de/sport/loewen-frankfurt/"),
]

headers = {"User-Agent": "LoewenNewsBot/1.0"}

for name, url in SCRAPE_SOURCES:
    try:
        # Quelle sicher anlegen
        cur.execute(
            "INSERT INTO sources (name, source_url) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (name, url)
        )
        cur.execute("SELECT id FROM sources WHERE source_url=%s", (url,))
        row = cur.fetchone()
        if not row:
            continue
        sid = row["id"]

        # HTML laden
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        links = soup.select("a[href]")[:15]

        for a in links:
            title = a.get_text(strip=True)
            link = a.get("href")

            if not title:
                continue
            if "löwen" not in title.lower():
                continue

            if link.startswith("/"):
                link = url.rstrip("/") + link

            cur.execute("""
                INSERT INTO news (title, source_url, source_id, category)
                VALUES (%s,%s,%s,'loewen_frankfurt')
                ON CONFLICT DO NOTHING
            """, (title, link, sid))

            inserted += cur.rowcount

    except Exception as e:
        # ❗ Wichtig: Fehler nur loggen, NICHT abbrechen
        print(f"[Phase3][{name}] skipped due to error:", e)
        continue
