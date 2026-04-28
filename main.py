from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser

app = FastAPI(title="Löwen Frankfurt News")


# =====================================================
# DB Connection
# =====================================================

def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )


# =====================================================
# Health
# =====================================================

@app.get("/")
def root():
    return {"app": "Löwen Frankfurt News", "status": "running ✅"}

@app.get("/health")
def health():
    return {"ok": True}


# =====================================================
# Setup + Migration (idempotent)
# =====================================================

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_url TEXT UNIQUE,
            source_id INTEGER,
            category TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"setup": "ok ✅"}


# =====================================================
# News API
# =====================================================

@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            news.id,
            news.title,
            news.content,
            news.created_at,
            news.category,
            sources.name AS source_name
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
        SELECT
            news.id,
            news.title,
            news.content,
            news.created_at,
            sources.name AS source_name
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
# RSS Import – NUR team-spezifische Feeds
# =====================================================

@app.get("/rss/import")
def import_rss():
    """
    Alle diese Feeds sind EXPLIZIT zu den Löwen Frankfurt.
    → Kategorie wird immer erzwungen.
    """

    FEEDS = [
        (
            "sport.de – Löwen Frankfurt",
            "https://www.sport.de/rss/news/te2940/loewen-frankfurt/",
        ),
        (
            "Eishockey NEWS – Löwen Frankfurt",
            "https://www.eishockeynews.de/rss/loewen-frankfurt",
        ),
    ]

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    processed = 0

    for source_name, feed_url in FEEDS:
        # Quelle anlegen
        cur.execute("""
            INSERT INTO sources (name, feed_url)
            VALUES (%s, %s)
            ON CONFLICT (feed_url) DO NOTHING;
        """, (source_name, feed_url))

        cur.execute(
            "SELECT id FROM sources WHERE feed_url = %s;",
            (feed_url,)
        )
        source_id = cur.fetchone()["id"]

        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            title = (entry.get("title") or "").strip()
            link = entry.get("link")
            content = (
                entry.get("summary")
                or entry.get("description")
                or ""
            ).strip()

            if not title or not link:
                continue

            # 🔥 Kategorie direkt setzen
            category = "loewen_frankfurt"

            cur.execute("""
                INSERT INTO news (title, content, source_url, source_id, category)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_url)
                DO UPDATE SET category = EXCLUDED.category;
            """, (title, content, link, source_id, category))

            processed += 1
            if cur.rowcount > 0:
                inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    return {
        "sources": len(FEEDS),
        "processed": processed,
        "inserted": inserted,
        "category": "loewen_frankfurt",
    }
