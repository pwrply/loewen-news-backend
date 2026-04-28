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
