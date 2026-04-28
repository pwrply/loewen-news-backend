from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser

app = FastAPI(title="Löwen Frankfurt News")


def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )


@app.get("/")
def root():
    return {"app": "Löwen Frankfurt News", "status": "running ✅"}


@app.get("/health")
def health():
    return {"ok": True}


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


@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            news.id,
            news.title,
            news.content,
            news.category,
            news.created_at,
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


@app.get("/rss/import")
def import_rss():
    FEEDS = [
        ("Heise", "https://www.heise.de/rss/heise-atom.xml"),
        ("Tagesschau", "https://www.tagesschau.de/xml/rss2"),
        ("Hessenschau", "https://www.hessenschau.de/index.rss"),
    ]

    FORCE_LOEWEN_SOURCES = {"Hessenschau", "Tagesschau"}

    TEAM_KEYWORDS = ["eishockey", "del", "tor", "playoff"]
    REGION_KEYWORDS = ["frankfurt", "hessen", "rhein-main"]

    conn = get_db_connection()
    cur = conn.cursor()

    changed = 0

    for source_name, feed_url in FEEDS:
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
            content = (entry.get("summary") or entry.get("description") or "").strip()

            if not title or not link:
                continue

            text = f"{title} {content}".lower()

            if source_name in FORCE_LOEWEN_SOURCES:
                category = "loewen_frankfurt"
            elif any(k in text for k in TEAM_KEYWORDS) or any(k in text for k in REGION_KEYWORDS):
                category = "loewen_frankfurt"
            else:
                category = "general"

            # 🔥 ENTSCHEIDENDER FIX: DO UPDATE
            cur.execute("""
                INSERT INTO news (title, content, source_url, source_id, category)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_url)
                DO UPDATE SET category = EXCLUDED.category;
            """, (title, content, link, source_id, category))

            changed += 1

    conn.commit()
    cur.close()
    conn.close()

    return {"processed": changed}
