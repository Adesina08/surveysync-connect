from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from urllib.parse import urlparse

import httpx
import xml.etree.ElementTree as ElementTree


@dataclass
class SessionInfo:
    token: str
    expires_at: datetime
    username: str
    password: str
    server_url: str


@dataclass
class SurveyCTOForm:
    form_id: str
    title: str
    version: str


_SESSIONS: Dict[str, SessionInfo] = {}


class SurveyCTOServiceError(Exception):
    pass


class InvalidSessionError(SurveyCTOServiceError):
    pass


class AuthenticationError(SurveyCTOServiceError):
    pass


class ApiAccessError(SurveyCTOServiceError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ServerConnectionError(SurveyCTOServiceError):
    pass


class FormListParseError(SurveyCTOServiceError):
    pass


def _normalize_server_url(server_url: str) -> str:
    parsed = urlparse(server_url)
    if not parsed.scheme:
        return f"https://{server_url.strip('/')}"
    return server_url.rstrip("/")


def _parse_form_list(xml_payload: str) -> list[SurveyCTOForm]:
    try:
        root = ElementTree.fromstring(xml_payload)
    except ElementTree.ParseError as exc:
        raise FormListParseError("Unable to parse SurveyCTO form list response.") from exc

    forms: list[SurveyCTOForm] = []
    for xform in root.findall(".//{*}xform"):
        form_id = xform.findtext(".//{*}formID")
        title = xform.findtext(".//{*}name")
        version = xform.findtext(".//{*}version")

        if not form_id:
            continue

        resolved_title = (title or form_id).strip()
        forms.append(
            SurveyCTOForm(
                form_id=form_id.strip(),
                title=resolved_title,
                version=(version or "").strip(),
            )
        )
    return forms


def create_session(username: str, password: str, server_url: str) -> SessionInfo:
    normalized_url = _normalize_server_url(server_url)
    token = f"session_{username}_{int(datetime.now(tz=timezone.utc).timestamp())}"
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    session = SessionInfo(
        token=token,
        expires_at=expires_at,
        username=username,
        password=password,
        server_url=normalized_url,
    )
    _SESSIONS[token] = session
    return session


def get_session(session_token: str) -> SessionInfo:
    session = _SESSIONS.get(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")
    if session.expires_at < datetime.now(tz=timezone.utc):
        _SESSIONS.pop(session_token, None)
        raise InvalidSessionError("Session token is expired.")
    return session


async def _fetch_form_list(session: SessionInfo) -> str:
    form_list_url = f"{session.server_url}/formList"
    headers = {
        "X-OpenRosa-Version": "1.0",
        "Accept": "text/xml, application/xml",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(form_list_url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError("SurveyCTO form list endpoint was not found on this server.", status_code=404)
    if response.status_code >= 400:
        raise ApiAccessError(f"SurveyCTO form list request failed with status {response.status_code}.", response.status_code)

    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""
    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        raise FormListParseError("SurveyCTO /formList did not return XML.")
    return body


async def _fetch_form_ids(session: SessionInfo) -> list[str]:
    url = f"{session.server_url}/api/v2/forms/ids"
    headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError("SurveyCTO forms ids endpoint was not found on this server.", status_code=404)
    if response.status_code >= 400:
        raise ApiAccessError(f"SurveyCTO forms ids request failed with status {response.status_code}.", response.status_code)

    # HARD GUARD: SurveyCTO sometimes returns HTML with 200
    ctype = (response.headers.get("content-type") or "").lower()
    if "json" not in ctype:
        snippet = (response.text or "")[:300]
        raise FormListParseError(f"Expected JSON from forms/ids but got content-type={ctype}. Snippet={snippet!r}")

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = (response.text or "")[:300]
        raise FormListParseError(f"Invalid JSON from forms/ids. Snippet={snippet!r}") from exc

    if isinstance(payload, dict) and isinstance(payload.get("formIds"), list):
        return [str(x).strip() for x in payload["formIds"] if str(x).strip()]

    raise FormListParseError(f"Unexpected forms/ids JSON structure. Keys={list(payload.keys()) if isinstance(payload, dict) else type(payload)}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    session = get_session(session_token)

    # prefer /formList for titles
    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        pass

    # fallback ids-only
    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=fid, title=fid, version="") for fid in form_ids]


def _dt_to_epoch_seconds(dt: datetime | None) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


async def fetch_submissions_wide_json(
    session_token: str,
    form_id: str,
    since_dt: datetime | None,
) -> list[dict[str, Any]]:
    """
    Equivalent to your script:
      /api/v2/forms/data/wide/json/{FORM_ID}?date=<epoch_seconds>
    """
    session = get_session(session_token)

    date_param = _dt_to_epoch_seconds(since_dt)
    url = f"{session.server_url}/api/v2/forms/data/wide/json/{form_id}?date={date_param}"

    headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
            resp = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server for submissions.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if resp.status_code >= 400:
        # Surface a snippet to help debug 412/417/500 etc.
        snippet = (resp.text or "")[:300]
        raise ApiAccessError(
            f"SurveyCTO submissions request failed with status {resp.status_code}. Snippet={snippet!r}",
            status_code=resp.status_code,
        )

    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" not in ctype:
        snippet = (resp.text or "")[:300]
        raise FormListParseError(f"Expected JSON submissions but got content-type={ctype}. Snippet={snippet!r}")

    try:
        data = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:300]
        raise FormListParseError(f"Invalid JSON from submissions endpoint. Snippet={snippet!r}") from exc

    if not isinstance(data, list):
        raise FormListParseError(f"Unexpected submissions JSON type: {type(data).__name__}")

    # ensure each item is a dict
    rows: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)
    return rows
