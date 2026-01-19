from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
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
        raise FormListParseError("Unable to parse SurveyCTO form list response (invalid XML).") from exc

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
    """
    OpenRosa endpoint /formList (best for titles).
    Can return HTML/login pages with 200; we guard against non-XML.
    """
    url = f"{session.server_url}/formList"
    headers = {
        "X-OpenRosa-Version": "1.0",
        "Accept": "text/xml, application/xml",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            resp = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if resp.status_code == 404:
        raise ApiAccessError("SurveyCTO /formList endpoint not found on this server.", status_code=404)
    if resp.status_code >= 400:
        raise ApiAccessError(f"SurveyCTO /formList failed with status {resp.status_code}.", status_code=resp.status_code)

    content_type = (resp.headers.get("content-type") or "").lower()
    body = resp.text or ""

    # Must be XML-ish
    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        snippet = body[:300].replace("\n", " ")
        raise FormListParseError(
            f"SurveyCTO /formList did not return XML. content-type={content_type!r} snippet={snippet!r}"
        )

    return body


async def _fetch_form_ids(session: SessionInfo) -> list[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    Detects non-JSON 200 responses and returns useful snippet.
    """
    url = f"{session.server_url}/api/v2/forms/ids"
    headers = {"Accept": "application/json", "User-Agent": "SurveySync Connect"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
            resp = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if resp.status_code == 404:
        raise ApiAccessError("SurveyCTO /api/v2/forms/ids not found on this server.", status_code=404)
    if resp.status_code >= 400:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise ApiAccessError(
            f"SurveyCTO forms ids request failed with status {resp.status_code}. snippet={snippet!r}",
            status_code=resp.status_code,
        )

    # Must be JSON content-type, but some servers return HTML with 200
    content_type = (resp.headers.get("content-type") or "").lower()
    if "json" not in content_type:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise FormListParseError(
            f"SurveyCTO forms ids returned non-JSON. content-type={content_type!r} snippet={snippet!r}"
        )

    try:
        payload: Any = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise FormListParseError(
            f"SurveyCTO forms ids returned invalid JSON. content-type={content_type!r} snippet={snippet!r}"
        ) from exc

    if isinstance(payload, dict):
        form_ids = payload.get("formIds")
        if isinstance(form_ids, list):
            return [str(x).strip() for x in form_ids if str(x).strip()]

        # SurveyCTO error format sometimes includes {error:{...}}
        if "error" in payload:
            raise ApiAccessError(f"SurveyCTO API error: {payload.get('error')}", status_code=resp.status_code)

        raise FormListParseError(f"SurveyCTO forms ids response missing 'formIds'. keys={list(payload.keys())}")

    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]

    raise FormListParseError(f"Unexpected JSON type from forms ids: {type(payload).__name__}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Prefer /formList for titles.
    Fall back to /api/v2/forms/ids (IDs only).
    """
    session = get_session(session_token)

    # Prefer /formList
    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        # fallback below
        pass

    # Fallback to v2 IDs
    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=fid, title=fid, version="") for fid in form_ids]


def _datetime_to_epoch_seconds(dt: datetime | None) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


async def fetch_submissions_wide_json(
    session_token: str,
    form_id: str,
    since_dt: datetime | None,
) -> list[dict]:
    """
    Fetch wide JSON submissions:
    GET /api/v2/forms/data/wide/json/{formId}?date={epoch_seconds}
    """
    session = get_session(session_token)
    date_param = _datetime_to_epoch_seconds(since_dt)
    url = f"{session.server_url}/api/v2/forms/data/wide/json/{form_id}?date={date_param}"

    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
            resp = await client.get(url, auth=(session.username, session.password), headers=headers)
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    # SurveyCTO docs mention enforced quiet period on full pulls; other preconditions can happen too
    if resp.status_code in {412, 417, 429}:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise SubmissionsFetchError(
            f"SurveyCTO submissions request blocked (status {resp.status_code}). snippet={snippet!r}",
            status_code=resp.status_code,
        )

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")
    if resp.status_code == 404:
        raise SubmissionsFetchError(f"SurveyCTO form data endpoint not found for form '{form_id}'.", status_code=404)
    if resp.status_code >= 400:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise SubmissionsFetchError(
            f"SurveyCTO submissions request failed with status {resp.status_code}. snippet={snippet!r}",
            status_code=resp.status_code,
        )

    content_type = (resp.headers.get("content-type") or "").lower()
    if "json" not in content_type:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise SubmissionsFetchError(
            f"SurveyCTO submissions returned non-JSON. content-type={content_type!r} snippet={snippet!r}",
            status_code=resp.status_code,
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        raise SubmissionsFetchError(f"SurveyCTO submissions returned invalid JSON. snippet={snippet!r}") from exc

    if not isinstance(payload, list):
        raise SubmissionsFetchError(f"Expected list JSON from SurveyCTO submissions, got {type(payload).__name__}")

    # Ensure each row is dict
    rows: list[dict] = []
    for item in payload:
        if isinstance(item, dict):
            rows.append(item)
    return rows
