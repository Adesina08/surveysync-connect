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

        if not form_id or not title:
            continue

        forms.append(
            SurveyCTOForm(
                form_id=form_id.strip(),
                title=title.strip(),
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
    Legacy OpenRosa endpoint. Often accessible, but can return HTML (login page, proxy page, etc.)
    which would break XML parsing unless we guard it.
    """
    form_list_url = f"{session.server_url}/formList"
    headers = {
        "X-OpenRosa-Version": "1.0",
        "Accept": "text/xml, application/xml",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
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

    # Guard against non-XML payloads (e.g., HTML login page) that still return 200
    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""
    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        raise FormListParseError("SurveyCTO /formList did not return XML.")

    return body


async def _fetch_form_ids(session: SessionInfo) -> Iterable[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    """
    form_ids_url = f"{session.server_url}/api/v2/forms/ids"
    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
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
        raise FormListParseError("Unable to parse SurveyCTO form ids response (invalid JSON).") from exc

    form_ids = payload.get("formIds")
    if not isinstance(form_ids, list):
        raise FormListParseError("Unable to parse SurveyCTO form ids response.")

    return [str(form_id).strip() for form_id in form_ids if str(form_id).strip()]


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Preferred behavior:
    1) Use SurveyCTO Server API v2 (/api/v2/forms/ids) first.
    2) If v2 doesn't exist (404), fall back to OpenRosa /formList (XML).
    This prevents 500s when /formList returns non-XML content with a 200.
    """
    session = _SESSIONS.get(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")

    # 1) Prefer v2 (JSON)
    try:
        form_ids = await _fetch_form_ids(session)
        return [
            SurveyCTOForm(
                form_id=form_id,
                title=form_id,   # API returns only IDs here; use ID as title
                version="",
            )
            for form_id in form_ids
        ]
    except ApiAccessError as exc:
        # Only fall back to /formList if v2 endpoint doesn't exist
        if exc.status_code != 404:
            raise

    # 2) Fallback: /formList (XML)
    xml_payload = await _fetch_form_list(session)
    return _parse_form_list(xml_payload)
