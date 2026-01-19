from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Any
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


class SubmissionsFetchError(SurveyCTOServiceError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


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


def get_session(session_token: str) -> SessionInfo:
    session = _SESSIONS.get(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")
    if session.expires_at < datetime.now(tz=timezone.utc):
        _SESSIONS.pop(session_token, None)
        raise InvalidSessionError("Session token is expired.")
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
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(form_list_url, auth=(session.username, session.password), headers=headers)
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
        snippet = (response.text or "")[:300]
        raise ApiAccessError(
            f"SurveyCTO form list request failed with status {response.status_code}. Snippet={snippet!r}",
            status_code=response.status_code,
        )

    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""

    # basic non-XML guard
    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        snippet = body[:300]
        raise FormListParseError(f"SurveyCTO /formList did not return XML. content-type={content_type} snippet={snippet!r}")

    return body


async def _fetch_form_ids(session: SessionInfo) -> Iterable[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    Robust against unexpected payloads (HTML returned with 200).
    """
    form_ids_url = f"{session.server_url}/api/v2/forms/ids"
    headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            response = await client.get(form_ids_url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if response.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if response.status_code == 404:
        raise ApiAccessError("SurveyCTO forms ids endpoint was not found on this server.", status_code=404)
    if response.status_code >= 400:
        snippet = (response.text or "")[:300]
        raise ApiAccessError(
            f"SurveyCTO forms ids request failed with status {response.status_code}. Snippet={snippet!r}",
            status_code=response.status_code,
        )

    content_type = (response.headers.get("content-type") or "").lower()
    if "json" not in content_type:
        snippet = (response.text or "")[:300]
        raise FormListParseError(
            f"SurveyCTO forms ids did not return JSON. content-type={content_type} snippet={snippet!r}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = (response.text or "")[:300]
        raise FormListParseError(f"Unable to parse SurveyCTO form ids response. snippet={snippet!r}") from exc

    if isinstance(payload, dict):
        form_ids = payload.get("formIds")
        if isinstance(form_ids, list):
            return [str(form_id).strip() for form_id in form_ids if str(form_id).strip()]

        raise FormListParseError(f"SurveyCTO forms ids response missing 'formIds'. Keys={list(payload.keys())}")

    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]

    raise FormListParseError(f"Unexpected JSON type from forms ids: {type(payload).__name__}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Prefer /formList (has titles).
    Fall back to v2 /api/v2/forms/ids (IDs only).
    """
    session = get_session(session_token)

    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        pass

    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=form_id, title=form_id, version="") for form_id in form_ids]


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
    Fetch wide JSON submissions:
      /api/v2/forms/data/wide/json/{form_id}?date=<epoch_seconds>
    """

    session = get_session(session_token)

    async def _do_fetch(date_seconds: int) -> httpx.Response:
        url = f"{session.server_url}/api/v2/forms/data/wide/json/{form_id}?date={date_seconds}"
        headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}
        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0), follow_redirects=True) as client:
            return await client.get(url, auth=(session.username, session.password), headers=headers)

    date_param = _dt_to_epoch_seconds(since_dt)

    try:
        resp = await _do_fetch(date_param)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server for submissions.") from exc

    # Handle common auth failures
    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")

    # IMPORTANT: SurveyCTO sometimes returns 412 for big/full pull. If first run (date=0),
    # retry with a safer window (e.g., last 30 days) to avoid precondition failures.
    if resp.status_code == 412 and since_dt is None:
        fallback_since = datetime.now(tz=timezone.utc) - timedelta(days=30)
        fallback_date = _dt_to_epoch_seconds(fallback_since)
        try:
            resp = await _do_fetch(fallback_date)
        except httpx.RequestError as exc:
            raise ServerConnectionError("Unable to reach the SurveyCTO server for submissions (retry).") from exc

    if resp.status_code >= 400:
        snippet = (resp.text or "")[:300]
        raise SubmissionsFetchError(
            f"SurveyCTO submissions request failed with status {resp.status_code}. Snippet={snippet!r}",
            status_code=resp.status_code,
        )

    content_type = (resp.headers.get("content-type") or "").lower()
    if "json" not in content_type:
        snippet = (resp.text or "")[:300]
        raise SubmissionsFetchError(
            f"SurveyCTO submissions did not return JSON. content-type={content_type} snippet={snippet!r}"
        )

    try:
        data = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:300]
        raise SubmissionsFetchError(f"Invalid JSON from submissions endpoint. Snippet={snippet!r}") from exc

    if not isinstance(data, list):
        raise SubmissionsFetchError(f"Unexpected submissions JSON type: {type(data).__name__}")

    rows: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)

    return rows
