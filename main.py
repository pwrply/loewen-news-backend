from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser

app = FastAPI(title="Löwen Frankfurt News")


# =====================================================
# Datenbank-Verbindung
# =====================================================

def get_db_connection():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )


# =====================================================
# Health / Basis
# =====================================================

@app.get("/")
def root():
    return {"app": "Löwen Frankfurt News", "status": "running ✅"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/db-test")
def db_test():
    try:
        conn = get_db_connection()
        conn.close()
        return {"database": "connected ✅"}
    except Exception as e:
        return {"database": "error ❌", "detail": str(e)}


# =====================================================
# Setup + Migration (sicher, wiederholbar)
# =====================================================

@app.get("/setup")
def setup():
    conn = get_db_connection()
    cur = conn.cursor()

    # RSS-Quellen
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            feed_url TEXT UNIQUE NOT NULL
        );
        """
    )

    # News-Tabelle (Basis)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Migrationen (für alte Datenbanken!)
    cur.execute("""ALTER TABLE news ADD COLUMN IF NOT EXISTS source_url TEXT;""")
    cur.execute("""ALTER TABLE news ADD COLUMN IF NOT EXISTS source_id INTEGER;""")

    # Unique Constraint für source_url
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'news_source_url_unique'
            ) THEN
                ALTER TABLE news
                ADD CONSTRAINT news_source_url_unique UNIQUE (source_url);
            END IF;
        END $$;
        """
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"setup": "sources & news ready ✅"}

@app.get("/migrate/news/category")
def migrate_news_category():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        ALTER TABLE news
        ADD COLUMN IF NOT EXISTS category TEXT;
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"migration": "category column added ✅"}


# =====================================================
# News API
# =====================================================

@app.get("/news")
def list_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            news.id,
            news.title,
            news.content,
            news.created_at,
            sources.name AS source_name
        FROM news
        LEFT JOIN sources ON news.source_id = sources.id
        ORDER BY news.created_at DESC;
        """
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


@app.get("/news/add")
def add_test_news():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO news (title, content)
        VALUES (%s, %s);
        """,
        (
            "Löwen Frankfurt gewinnen 4:2",
            "Starkes Heimspiel in der DEL, wichtige drei Punkte.",
        ),
    )

    conn.commit()
    cur.close()
    conn.close()
    return {"news": "added ✅"}


# =====================================================
# RSS-Import (mehrere Quellen, robust)
# =====================================================

@app.get("/rss/import")
def import_rss():
    FEEDS = [
        ("Heise", "https://www.heise.de/rss/heise-atom.xml"),
        ("Tagesschau", "https://www.tagesschau.de/xml/rss2"),
        ("Hessenschau", "https://www.hessenschau.de/index.rss"),
    ]

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for source_name, feed_url in FEEDS:
        # Quelle anlegen
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
        row = cur.fetchone()
        if not row:
            continue

        source_id = row["id"]
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            try:
                title = (entry.get("title") or "").strip()
                link = entry.get("link")

                content = (
                    entry.get("summary")
                    or entry.get("description")
                    or ""
                ).strip()

                if not title or not link:
                    skipped += 1
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
                    inserted += 1
                else:
                    skipped += 1

            except Exception:
                skipped += 1
                continue

    conn.commit()
    cur.close()
    conn.close()

    return {
        "sources": len(FEEDS),
        "inserted": inserted,
        "skipped": skipped,
    }
