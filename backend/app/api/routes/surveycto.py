from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel

from backend.app.services import surveycto_service

router = APIRouter(prefix="/surveycto", tags=["surveycto"])


class SurveyCTOFormResponse(BaseModel):
    form_id: str
    title: str
    version: str


@router.get("/forms", response_model=list[SurveyCTOFormResponse])
def list_forms(session_token: str = Query(..., description="Session token from /sessions")) -> list[SurveyCTOFormResponse]:
    forms = surveycto_service.list_forms(session_token)
    return [SurveyCTOFormResponse(form_id=form.form_id, title=form.title, version=form.version) for form in forms]
