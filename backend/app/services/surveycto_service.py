from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable
from urllib.parse import urlparse
import xml.etree.ElementTree as ElementTree

import httpx


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
    """
    Parse OpenRosa /formList XML into SurveyCTOForm objects.
    """
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

        # if name is missing, fall back to formID for title
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


async def _fetch_form_list(session: SessionInfo) -> str:
    """
    OpenRosa endpoint /formList.
    Can return HTML/login pages with 200; we guard against non-XML.
    """
    form_list_url = f"{session.server_url}/formList"
    headers = {
        "X-OpenRosa-Version": "1.0",
        "Accept": "text/xml, application/xml",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(
                form_list_url,
                auth=(session.username, session.password),
                headers=headers,
            )
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError(
            "SurveyCTO form list endpoint was not found on this server.",
            status_code=response.status_code,
        )
    if response.status_code >= 400:
        raise ApiAccessError(
            f"SurveyCTO form list request failed with status {response.status_code}.",
            status_code=response.status_code,
        )

    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""

    # basic non-XML guard
    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        raise FormListParseError("SurveyCTO /formList did not return XML.")

    return body


async def _fetch_form_ids(session: SessionInfo) -> Iterable[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    Robust against unexpected JSON shapes.
    """
    form_ids_url = f"{session.server_url}/api/v2/forms/ids"
    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(
                form_ids_url,
                auth=(session.username, session.password),
                headers=headers,
            )
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError(
            "SurveyCTO forms ids endpoint was not found on this server.",
            status_code=response.status_code,
        )
    if response.status_code >= 400:
        raise ApiAccessError(
            f"SurveyCTO forms ids request failed with status {response.status_code}.",
            status_code=response.status_code,
        )

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = (response.text or "")[:300]
        raise FormListParseError(
            "SurveyCTO forms ids returned invalid JSON. "
            f"content-type={response.headers.get('content-type')} snippet={snippet!r}"
        ) from exc

    if isinstance(payload, dict):
        form_ids = payload.get("formIds")
        if isinstance(form_ids, list):
            return [str(x).strip() for x in form_ids if str(x).strip()]

        if "error" in payload:
            raise ApiAccessError(f"SurveyCTO API error: {payload.get('error')}")

        raise FormListParseError(
            f"SurveyCTO forms ids response missing 'formIds' list. Keys={list(payload.keys())}"
        )

    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]

    raise FormListParseError(f"Unexpected JSON type from forms ids: {type(payload).__name__}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Prefer /formList (has titles).
    Fall back to v2 /api/v2/forms/ids (IDs only).
    """
    session = _SESSIONS.get(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")

    # 1) Prefer OpenRosa /formList for titles
    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        # fall back to v2 below
        pass

    # 2) Fallback to v2 (IDs only)
    form_ids = await _fetch_form_ids(session)
    return [
        SurveyCTOForm(form_id=form_id, title=form_id, version="")
        for form_id in form_ids
    ]
