from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import feedparser

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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS source_url TEXT;")
    cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS source_id INTEGER;")
    cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS category TEXT;")

    cur.execute("""
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
# RSS Import (STABIL & PRAXISNAH)
# =====================================================

@app.get("/rss/import")
def import_rss():
    FEEDS = [
        ("Heise", "https://www.heise.de/rss/heise-atom.xml"),
        ("Tagesschau", "https://www.tagesschau.de/xml/rss2"),
        ("Hessenschau", "https://www.hessenschau.de/index.rss"),
    ]

    # ✅ Diese Quellen sind regional / sportlich relevant → erzwingen Löwen
    FORCE_LOEWEN_SOURCES = {
        "Hessenschau",
        "Tagesschau",
    }

    # ✅ Keyword-Fallback (entschärft)
    TEAM_KEYWORDS = ["eishockey", "del", "tor", "playoff", "heimspiel"]
    REGION_KEYWORDS = ["frankfurt", "hessen", "rhein-main"]

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0
    loewen_count = 0

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
        src = cur.fetchone()
        if not src:
            continue

        source_id = src["id"]
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

                text = f"{title} {content}".lower()

                # ✅ Kategorie bestimmen
                if source_name in FORCE_LOEWEN_SOURCES:
                    category = "loewen_frankfurt"
                    loewen_count += 1
                else:
                    team_match = any(k in text for k in TEAM_KEYWORDS)
                    region_match = any(k in text for k in REGION_KEYWORDS)
                    if team_match or region_match:
                        category = "loewen_frankfurt"
                        loewen_count += 1
                    else:
                        category = "general"

                cur.execute("""
                    INSERT INTO news (title, content, source_url, source_id, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO NOTHING;
                """, (title, content, link, source_id, category))

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
        "loewen_news": loewen_count,
        "skipped": skipped,
    }
