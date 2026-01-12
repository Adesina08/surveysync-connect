from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict


@dataclass
class SessionInfo:
    token: str
    expires_at: datetime


@dataclass
class SurveyCTOForm:
    form_id: str
    title: str
    version: str


_SESSIONS: Dict[str, SessionInfo] = {}


def create_session(username: str, password: str, server_url: str) -> SessionInfo:
    token = f"session_{username}_{int(datetime.now(tz=timezone.utc).timestamp())}"
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    session = SessionInfo(token=token, expires_at=expires_at)
    _SESSIONS[token] = session
    return session


def list_forms(session_token: str) -> list[SurveyCTOForm]:
    if session_token not in _SESSIONS:
        return []
    return [
        SurveyCTOForm(form_id="household_survey", title="Household Survey", version="v1"),
        SurveyCTOForm(form_id="facility_assessment", title="Facility Assessment", version="v3"),
    ]
