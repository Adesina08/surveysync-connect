from fastapi import FastAPI

from backend.app.api.routes import postgres, sessions, surveycto, sync_jobs
from backend.app.db.session import init_db

app = FastAPI(title="SurveySync Connect Backend")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


app.include_router(sessions.router)
app.include_router(surveycto.router)
app.include_router(postgres.router)
app.include_router(sync_jobs.router)
