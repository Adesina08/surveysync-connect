from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable
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
    """
    form_ids_url = f"{session.server_url}/api/v2/forms/ids"
    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
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

    form_ids = payload.get("formIds") if isinstance(payload, dict) else None
    if not isinstance(form_ids, list):
        raise FormListParseError("Unable to parse SurveyCTO form ids response.")

    return [str(form_id).strip() for form_id in form_ids if str(form_id).strip()]


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
        pass

    # 2) Fallback to v2 (IDs only)
    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=form_id, title=form_id, version="") for form_id in form_ids]


def _datetime_to_epoch_seconds(dt: datetime | None) -> int:
    """
    Convert datetime to Unix epoch seconds (UTC) for SurveyCTO date parameter.
    If dt is None => full pull => date=0 (subject to SurveyCTO quiet-period rules).
    """
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return int(dt_utc.timestamp())


def _extract_surveycto_error_message(resp: httpx.Response) -> str | None:
    """
    Best-effort: SurveyCTO errors sometimes come as JSON:
      { "error": { "code": ..., "message": "...", ... } }
    """
    try:
        payload = resp.json()
    except Exception:
        return None

    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
        # sometimes: {"detail": "..."} etc
        for k in ("message", "detail"):
            v = payload.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


async def fetch_wide_json_submissions(
    session_token: str,
    form_id: str,
    since_dt: datetime | None,
) -> list[dict[str, Any]]:
    """
    Fetch submissions from:
      /api/v2/forms/data/wide/json/{form_id}?date={epoch_seconds}

    Handles SurveyCTO's "quiet period" / precondition behavior (commonly 412/417)
    and rate limiting (429) with clear error messages.
    """
    session = _SESSIONS.get(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")

    date_param = _datetime_to_epoch_seconds(since_dt)
    url = f"{session.server_url}/api/v2/forms/data/wide/json/{form_id}"
    params = {"date": str(date_param)}

    headers = {
        "Accept": "application/json",
        "User-Agent": "SurveySync Connect",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
            resp = await client.get(
                url,
                params=params,
                headers=headers,
                auth=(session.username, session.password),
            )
    except httpx.RequestError as exc:
        raise ServerConnectionError("Unable to reach the SurveyCTO server.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")

    # SurveyCTO full-pull quiet period / precondition behavior
    # (your earlier script handled 417; in practice some servers return 412 too)
    if resp.status_code in {412, 417}:
        msg = _extract_surveycto_error_message(resp)
        extra = ""
        if date_param == 0:
            extra = (
                " SurveyCTO may enforce a quiet period for full pulls (date=0). "
                "Wait a few minutes and try again."
            )
        raise ApiAccessError(
            f"SurveyCTO temporarily refused the request (status {resp.status_code})."
            + (f" {msg}" if msg else "")
            + extra,
            status_code=resp.status_code,
        )

    if resp.status_code == 429:
        msg = _extract_surveycto_error_message(resp)
        raise ApiAccessError(
            f"SurveyCTO rate limit hit (429)."
            + (f" {msg}" if msg else " Please retry shortly."),
            status_code=resp.status_code,
        )

    if resp.status_code >= 400:
        msg = _extract_surveycto_error_message(resp)
        raise ApiAccessError(
            f"SurveyCTO submissions request failed with status {resp.status_code}."
            + (f" {msg}" if msg else ""),
            status_code=resp.status_code,
        )

    try:
        data = resp.json()
    except ValueError as exc:
        raise FormListParseError("SurveyCTO submissions response was not valid JSON.") from exc

    if not isinstance(data, list):
        raise FormListParseError("SurveyCTO wide JSON endpoint returned unexpected JSON shape (expected list).")

    rows: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)
    return rows
