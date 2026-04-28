from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser

app = FastAPI(title="Löwen Frankfurt News")


# ---------------------------
# Datenbank-Verbindung
# ---------------------------

def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor
    )


# ---------------------------
# Basis-Routen
# ---------------------------

@app.get("/")
def root():
    return {
        "app": "Löwen Frankfurt News",
        "message": "Backend läuft ✅"
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


# ---------------------------
# Datenbank-Setup
# ---------------------------

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    # Quellen (RSS-Feeds)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT UNIQUE NOT NULL
        );
        """
    )

    # News-Artikel
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            source_url TEXT UNIQUE,
            source_id INTEGER REFERENCES sources(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"setup": "sources & news tables ready ✅"}



# ---------------------------
# News anzeigen
# ---------------------------

@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM news ORDER BY created_at DESC;"
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


# ---------------------------
# RSS-Import (crash-sicher)
# ---------------------------

@app.get("/rss/import")
def import_rss():
    FEEDS = [
        ("Heise", "https://www.heise.de/rss/heise-atom.xml"),
        ("Tagesschau", "https://www.tagesschau.de/xml/rss2"),
        ("Hessenschau", "https://www.hessenschau.de/index.rss"),
    ]

    conn = get_db_connection()
    cur = conn.cursor()

    total_inserted = 0
    total_skipped = 0

    for source_name, feed_url in FEEDS:
        # Quelle anlegen (falls noch nicht da)
        cur.execute(
            """
            INSERT INTO sources (name, feed_url)
            VALUES (%s, %s)
            ON CONFLICT (feed_url) DO NOTHING;
            """,
            (source_name, feed_url),
        )

        # source_id holen
        cur.execute(
            "SELECT id FROM sources WHERE feed_url = %s;",
            (feed_url,),
        )
        source_id_row = cur.fetchone()
        if not source_id_row:
            continue

        source_id = source_id_row["id"]

        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            try:
                title = (entry.get("title") or "").strip()
                link = entry.get("link")

                content = ""
                if "summary" in entry:
                    content = entry.summary
                elif "description" in entry:
                    content = entry.description
                elif "content" in entry and isinstance(entry.content, list):
                    first = entry.content[0]
                    if isinstance(first, dict):
                        content = first.get("value", "")

                content = (content or "").strip()

                if not title or not link:
                    total_skipped += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO news (title, content, source_url, source_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_url) DO NOTHING;
                    """,
                    (title, content, link, source_id),
                )

                if cur.rowcount > 0:
                    total_inserted += 1
                else:
                    total_skipped += 1

            except Exception:
                total_skipped += 1
                continue

    conn.commit()
    cur.close()
    conn.close()

    return {
        "inserted": total_inserted,
        "skipped": total_skipped,
        "sources": len(FEEDS),
    }
