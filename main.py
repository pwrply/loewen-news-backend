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
