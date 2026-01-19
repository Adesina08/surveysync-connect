from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Any
from urllib.parse import urlparse, quote

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


class SubmissionsFetchError(SurveyCTOServiceError):
    pass


def _normalize_server_url(server_url: str) -> str:
    parsed = urlparse(server_url)
    if not parsed.scheme:
        return f"https://{server_url.strip('/')}"
    return server_url.rstrip("/")


def _epoch_seconds(dt: datetime | None) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


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
    if session.expires_at <= datetime.now(tz=timezone.utc):
        _SESSIONS.pop(session_token, None)
        raise InvalidSessionError("Session token is expired.")
    return session


async def _fetch_form_list(session: SessionInfo) -> str:
    """
    OpenRosa endpoint /formList.
    Sometimes returns HTML/login page with 200; we guard against non-XML.
    """
    form_list_url = f"{session.server_url}/formList"
    headers = {
        "X-OpenRosa-Version": "1.0",
        "Accept": "text/xml, application/xml",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
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
        raise ApiAccessError("SurveyCTO /formList endpoint was not found on this server.", 404)
    if response.status_code >= 400:
        raise ApiAccessError(f"SurveyCTO /formList request failed with status {response.status_code}.", response.status_code)

    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""

    # Stronger guard: HTML pages often start with "<!DOCTYPE html" or "<html"
    if "xml" not in content_type:
        lowered = body.lstrip().lower()
        if lowered.startswith("<!doctype html") or lowered.startswith("<html"):
            raise FormListParseError("SurveyCTO /formList returned HTML instead of XML (likely a login/proxy page).")

    return body


async def _fetch_form_ids(session: SessionInfo) -> Iterable[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    Some servers may not have it; some return HTML.
    """
    url = f"{session.server_url}/api/v2/forms/ids"
    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError("SurveyCTO forms ids endpoint was not found on this server.", 404)
    if response.status_code >= 400:
        raise ApiAccessError(f"SurveyCTO forms ids request failed with status {response.status_code}.", response.status_code)

    # If we got HTML, treat it like "not available" so caller can fallback
    content_type = (response.headers.get("content-type") or "").lower()
    text = response.text or ""
    if "json" not in content_type:
        lowered = text.lstrip().lower()
        if lowered.startswith("<!doctype html") or lowered.startswith("<html") or lowered.startswith("<"):
            raise ApiAccessError("SurveyCTO forms ids did not return JSON (likely not supported).", 404)

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = text[:300]
        raise FormListParseError(f"Unable to parse SurveyCTO form ids response (invalid JSON). snippet={snippet!r}") from exc

    if isinstance(payload, dict):
        form_ids = payload.get("formIds")
        if isinstance(form_ids, list):
            return [str(x).strip() for x in form_ids if str(x).strip()]
        raise FormListParseError("Unable to parse SurveyCTO form ids response: missing 'formIds' list.")

    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]

    raise FormListParseError(f"Unexpected JSON type from forms ids: {type(payload).__name__}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Prefer /formList (has titles).
    Fall back to /api/v2/forms/ids (IDs only).
    """
    session = get_session(session_token)

    # 1) Prefer OpenRosa /formList for titles
    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        pass

    # 2) Fallback to v2 (IDs only)
    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=fid, title=fid, version="") for fid in form_ids]


async def fetch_submissions_wide_json(
    session_token: str,
    form_id: str,
    since_dt: datetime | None,
) -> list[dict[str, Any]]:
    """
    Fetch submissions for a form using:
      /api/v2/forms/data/wide/json/{FORM_ID}?date={epoch_seconds}

    Handles SurveyCTO constraints:
    - If full pull is blocked (412/417), automatically retries with a safer window (last 30 days).
    """
    session = get_session(session_token)

    # Try incremental first (or 0 for first run)
    date_param = _epoch_seconds(since_dt)

    async def _do_request(date_value: int) -> httpx.Response:
        safe_form_id = quote(form_id, safe="")
        url = f"{session.server_url}/api/v2/forms/data/wide/json/{safe_form_id}?date={date_value}"
        headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
            return await client.get(url, auth=(session.username, session.password), headers=headers)

    try:
        resp = await _do_request(date_param)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")

    # If full pull blocked / precondition issues, retry last 30 days
    if resp.status_code in {412, 417}:
        fallback_dt = datetime.now(tz=timezone.utc) - timedelta(days=30)
        fallback_epoch = _epoch_seconds(fallback_dt)
        try:
            resp = await _do_request(fallback_epoch)
        except httpx.RequestError as exc:
            raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if resp.status_code >= 400:
        snippet = (resp.text or "")[:500]
        raise SubmissionsFetchError(f"SurveyCTO submissions request failed with status {resp.status_code}. {snippet}")

    # Validate JSON
    try:
        payload = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:500]
        raise SubmissionsFetchError(f"SurveyCTO submissions returned invalid JSON. snippet={snippet!r}") from exc

    # SurveyCTO returns a list of rows for wide/json
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]

    # Some proxies wrap data
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]

    raise SubmissionsFetchError("SurveyCTO submissions response format is unexpected.")
