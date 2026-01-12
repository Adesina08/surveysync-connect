from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import surveycto_service

router = APIRouter(prefix="/sessions", tags=["sessions"])


class SessionCreateRequest(BaseModel):
    username: str = Field(..., examples=["enumerator"])
    password: str = Field(..., examples=["secret"])
    server_url: str = Field(..., examples=["https://surveycto.example.com"])


class SessionResponse(BaseModel):
    session_token: str
    expires_at: datetime


@router.post("", response_model=SessionResponse)
def create_session(request: SessionCreateRequest) -> SessionResponse:
    session = surveycto_service.create_session(
        username=request.username,
        password=request.password,
        server_url=request.server_url,
    )
    return SessionResponse(session_token=session.token, expires_at=session.expires_at)
