import os
import traceback
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
# SETUP (passt zur EXISTIERENDEN DB)
# =====================================================

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    # sources hat feed_url als kanonische Spalte
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT NOT NULL UNIQUE
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

@app.get("/")
def root():
    return {"status": "running ✅"}


@app.get("/news/loewen")
def get_loewen_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT news.*, sources.name AS source_name
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        WHERE news.category = 'loewen_frankfurt'
        ORDER BY news.created_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# =====================================================
# RSS IMPORT – ABSOLUT 500‑SICHER
# =====================================================

@app.get("/rss/import")
def rss_import():
    inserted = 0
    errors = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # =============================
        # PHASE 1 – Team RSS
        # =============================
        TEAM_FEEDS = [
            ("sport.de – Löwen Frankfurt",
             "https://www.sport.de/rss/news/te2940/loewen-frankfurt/"),
            ("Eishockey NEWS – Löwen Frankfurt",
             "https://www.eishockeynews.de/del/loewen-frankfurt"),
        ]

        for name, feed_url in TEAM_FEEDS:
            try:
                cur.execute("""
                    INSERT INTO sources (name, feed_url)
                    VALUES (%s,%s)
                    ON CONFLICT (feed_url) DO NOTHING
                """, (name, feed_url))

                cur.execute("SELECT id FROM sources WHERE feed_url=%s", (feed_url,))
                sid = cur.fetchone()["id"]

                feed = feedparser.parse(feed_url)
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

            except Exception as e:
                errors.append(f"[PHASE1:{name}] {repr(e)}")

        # =============================
        # PHASE 2 – Eisblog
        # =============================
        try:
            name = "Eisblog"
            feed_url = "https://eisblog.media/feed/"
            KEYWORDS = ["löwen", "loewen", "frankfurt"]

            cur.execute("""
                INSERT INTO sources (name, feed_url)
                VALUES (%s,%s)
                ON CONFLICT (feed_url) DO NOTHING
            """, (name, feed_url))

            cur.execute("SELECT id FROM sources WHERE feed_url=%s", (feed_url,))
            sid = cur.fetchone()["id"]

            feed = feedparser.parse(feed_url)
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
                    ON CONFLICT DO NOTHING
                """, (title, content, link, sid))

                inserted += cur.rowcount

        except Exception as e:
            errors.append(f"[PHASE2:EISBLOG] {repr(e)}")

        # =============================
        # PHASE 3 – FNP / OP (HTML)
        # =============================
        SCRAPE_SOURCES = [
            ("FNP", "https://www.fnp.de/sport/loewen-frankfurt/"),
            ("OP-Online", "https://www.op-online.de/sport/loewen-frankfurt/"),
        ]

        headers = {"User-Agent": "LoewenNewsBot/1.0"}

        for name, feed_url in SCRAPE_SOURCES:
            try:
                cur.execute("""
                    INSERT INTO sources (name, feed_url)
                    VALUES (%s,%s)
                    ON CONFLICT (feed_url) DO NOTHING
                """, (name, feed_url))

                cur.execute("SELECT id FROM sources WHERE feed_url=%s", (feed_url,))
                sid = cur.fetchone()["id"]

                resp = requests.get(feed_url, headers=headers, timeout=8)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.select("a[href]")[:20]:
                    title = a.get_text(strip=True)
                    link = a.get("href")

                    if not title or "löwen" not in title.lower():
                        continue

                    if link.startswith("/"):
                        link = feed_url.rstrip("/") + link

                    cur.execute("""
                        INSERT INTO news (title, source_url, source_id, category)
                        VALUES (%s,%s,%s,'loewen_frankfurt')
                        ON CONFLICT DO NOTHING
                    """, (title, link, sid))

                    inserted += cur.rowcount

            except Exception as e:
                errors.append(f"[PHASE3:{name}] {repr(e)}")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as fatal:
        return {
            "status": "fatal_error_caught",
            "error": repr(fatal),
            "trace": traceback.format_exc(),
        }

    return {
        "status": "ok ✅",
        "inserted": inserted,
        "errors": errors,
    }
