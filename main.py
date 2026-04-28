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
        "INSERT INTO news (title, content) VALUES (%s, %s);",
        (
            "Löwen Frankfurt gewinnen 4:2",
            "Starkes Heimspiel in der DEL, wichtige drei Punkte.",
        )
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"news": "added ✅"}

import feedparser

@app.get("/rss/import")
def import_rss():
    FEED_URL = "https://www.heise.de/rss/heise-atom.xml"  # 🔁 HIER SPÄTER ECHTE QUELLE EINTRAGEN

    feed = feedparser.parse(FEED_URL)

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        summary = entry.get("summary", "").strip()
        link = entry.get("link")

        if not title or not link:
            continue

        try:
            cur.execute(
                """
                INSERT INTO news (title, content, source_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_url) DO NOTHING;
                """,
                (title, summary, link),
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()

    return {"rss_imported": inserted}
