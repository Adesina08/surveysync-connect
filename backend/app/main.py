from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import postgres, sessions, surveycto, sync_jobs
from app.db.session import init_db

app.include_router(postgres.router)

app = FastAPI(title="SurveySync Connect Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",

        # 🔴 REPLACE THIS WITH YOUR ACTUAL NETLIFY URL
        # example: "https://surveysync-connect.netlify.app"
        "https://surveytosql.netlify.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup() -> None:
    init_db()

app.include_router(sessions.router)
app.include_router(surveycto.router)
app.include_router(postgres.router)
app.include_router(sync_jobs.router)
