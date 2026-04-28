from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Löwen Frankfurt News")


def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor
    )


@app.get("/")
def root():
    return {
        "app": "Löwen Frankfurt News",
        "message": "Backend läuft!"
    }


@app.get("/hello")
def hello():
    return {"hello": "Willkommen 🦁🏒"}


@app.get("/db-test")
def db_test():
    try:
        conn = get_db_connection()
        conn.close()
        return {"database": "connected ✅"}
    except Exception as e:
        return {"database": "error ❌", "detail": str(e)}


@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            source_url TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    return {"setup": "news table ready ✅"}


@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM news ORDER BY created_at DESC;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
    
@app.get("/news/add")
def add_news():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO news (title, content, source_url) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
        (
            "Test News aus der App",
            "Diese News wurde aus der iOS App ausgelöst.",
            None,
        )
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"news": "added ✅"}

import feedparser

import feedparser

@app.get("/rss/import")
def import_rss():
    FEED_URL = "https://www.heise.de/rss/heise-atom.xml"

    feed = feedparser.parse(FEED_URL)

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for entry in feed.entries:
        title = (entry.get("title") or "").strip()
        link = entry.get("link")

        # 🔹 Inhalt robust holen
        content = (
            entry.get("summary")
            or entry.get("description")
            or (
                entry.get("content")[0].get("value")
                if entry.get("content") else ""
            )
            or ""
        )
        content = content.strip()

        if not title or not link:
            skipped += 1
            continue

        cur.execute(
            """
            INSERT INTO news (title, content, source_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (source_url) DO NOTHING;
            """,
            (title, content, link),
        )

        if cur.rowcount > 0:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cur.close()
    conn.close()

    return {
        "feed_entries": len(feed.entries),
        "inserted": inserted,
        "skipped": skipped,
    }
