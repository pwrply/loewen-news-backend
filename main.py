from fastapi import FastAPI

app = FastAPI(title="Löwen Frankfurt News")

@app.get("/")
def root():
    return {
        "app": "Löwen Frankfurt News",
        "message": "Backend läuft!"
    }

@app.get("/hello")
def hello():
    return {"hello": "Willkommen 🦁🏒"}
import os
import psycopg2

@app.get("/db-test")
def db_test():
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        conn.close()
        return {"database": "connected ✅"}
    except Exception as e:
        return {"database": "error ❌", "detail": str(e)}
