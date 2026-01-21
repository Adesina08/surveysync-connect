from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlparse
import xml.etree.ElementTree as ElementTree
import json
import re
import asyncio

import httpx

from app.db.session import get_connection


# -------------------------
# Models
# -------------------------

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


# -------------------------
# Errors
# -------------------------

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


class SubmissionsRateLimitError(SubmissionsFetchError):
    """Raised when SurveyCTO asks the client to wait (HTTP 417).

    The retry_after_seconds field (if present) can be used by callers to set a
    local cooldown and provide better UX.
    """

    def __init__(self, message: str, retry_after_seconds: int | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


# -------------------------
# SQLite session persistence
# -------------------------

def _ensure_sessions_table() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS surveycto_sessions (
                token TEXT PRIMARY KEY,
                expires_at TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                server_url TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _save_session(session: SessionInfo) -> None:
    _ensure_sessions_table()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO surveycto_sessions
                (token, expires_at, username, password, server_url, created_at)
            VALUES
                (?, ?, ?, ?, ?, ?)
            """,
            (
                session.token,
                session.expires_at.isoformat(),
                session.username,
                session.password,
                session.server_url,
                datetime.now(tz=timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def _load_session(token: str) -> SessionInfo | None:
    _ensure_sessions_table()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT token, expires_at, username, password, server_url
            FROM surveycto_sessions
            WHERE token = ?
            """,
            (token,),
        ).fetchone()

    if not row:
        return None

    expires_at = datetime.fromisoformat(row["expires_at"])
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    return SessionInfo(
        token=row["token"],
        expires_at=expires_at,
        username=row["username"],
        password=row["password"],
        server_url=row["server_url"],
    )


def _delete_session(token: str) -> None:
    _ensure_sessions_table()
    with get_connection() as conn:
        conn.execute("DELETE FROM surveycto_sessions WHERE token = ?", (token,))
        conn.commit()


# -------------------------
# Helpers
# -------------------------

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


def _datetime_to_epoch_seconds(dt: datetime | None) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def _extract_retry_after_seconds(resp: httpx.Response) -> int | None:
    """
    SurveyCTO sometimes returns:
      {"error":{"code":417,"message":"Please wait for 106 seconds before retrying ..."}}

    Extracts the 106.
    """
    # Prefer JSON message if possible
    text = resp.text or ""
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            err = payload.get("error")
            if isinstance(err, dict):
                msg = str(err.get("message") or "")
                m = re.search(r"wait for\s+(\d+)\s+seconds", msg, flags=re.IGNORECASE)
                if m:
                    return int(m.group(1))
    except Exception:
        pass

    # Fallback to raw text
    m = re.search(r"wait for\s+(\d+)\s+seconds", text, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))

    return None


async def _get_once(client: httpx.AsyncClient, url: str, *, auth: tuple[str, str], headers: dict | None = None) -> httpx.Response:
    """Single SurveyCTO GET request.

    Important: we do NOT sleep/retry inside backend jobs when SurveyCTO returns
    HTTP 417. Instead we surface a SubmissionsRateLimitError so the caller can
    record a cooldown and the UI can instruct the user to retry later.
    """
    return await client.get(url, auth=auth, headers=headers)


# -------------------------
# Public API used by routes / sync
# -------------------------

def create_session(username: str, password: str, server_url: str) -> SessionInfo:
    normalized_url = _normalize_server_url(server_url)
    token = f"session_{username}_{int(datetime.now(tz=timezone.utc).timestamp())}"
    expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=6)

    session = SessionInfo(
        token=token,
        expires_at=expires_at,
        username=username,
        password=password,
        server_url=normalized_url,
    )

    _save_session(session)
    return session


def get_session(session_token: str) -> SessionInfo:
    session = _load_session(session_token)
    if not session:
        raise InvalidSessionError("Session token is invalid or expired.")

    now = datetime.now(tz=timezone.utc)
    if session.expires_at < now:
        _delete_session(session_token)
        raise InvalidSessionError("Session token is invalid or expired.")

    return session


# -------------------------
# SurveyCTO HTTP calls
# -------------------------

async def _fetch_form_list(session: SessionInfo) -> str:
    """
    OpenRosa endpoint /formList (has titles).
    Can return HTML/login pages with 200; guard against non-XML.
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
        raise ApiAccessError("SurveyCTO form list endpoint was not found on this server.", status_code=404)
    if response.status_code >= 400:
        raise ApiAccessError(
            f"SurveyCTO form list request failed with status {response.status_code}.",
            status_code=response.status_code,
        )

    content_type = (response.headers.get("content-type") or "").lower()
    body = response.text or ""

    if ("xml" not in content_type) and (not body.lstrip().startswith("<")):
        raise FormListParseError(
            f"SurveyCTO /formList did not return XML. content-type={content_type!r} "
            f"snippet={(body[:300])!r}"
        )

    return body


async def _fetch_form_ids(session: SessionInfo) -> Iterable[str]:
    """
    SurveyCTO Server API v2 endpoint: /api/v2/forms/ids
    Often blocked / returns HTML; we guard and show snippet.
    """
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
        raise ApiAccessError(
            f"SurveyCTO forms ids request failed with status {response.status_code}.",
            status_code=response.status_code,
        )

    content_type = (response.headers.get("content-type") or "").lower()
    raw = response.text or ""

    # Must be JSON; if not, show snippet (this is your current failing case)
    if "json" not in content_type:
        raise FormListParseError(
            f"SurveyCTO forms ids did not return JSON. content-type={content_type!r} snippet={(raw[:300])!r}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise FormListParseError(
            f"SurveyCTO forms ids returned invalid JSON. content-type={content_type!r} snippet={(raw[:300])!r}"
        ) from exc

    if isinstance(payload, dict):
        form_ids = payload.get("formIds")
        if isinstance(form_ids, list):
            return [str(x).strip() for x in form_ids if str(x).strip()]

        err = payload.get("error") or payload
        raise FormListParseError(f"SurveyCTO forms ids JSON missing formIds. payload={err}")

    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]

    raise FormListParseError(f"Unexpected JSON type from forms ids: {type(payload).__name__}")


async def list_forms(session_token: str) -> list[SurveyCTOForm]:
    """
    Prefer /formList (titles). Fall back to /api/v2/forms/ids (IDs only).
    """
    session = get_session(session_token)

    # 1) Prefer /formList (titles)
    try:
        xml_payload = await _fetch_form_list(session)
        forms = _parse_form_list(xml_payload)
        if forms:
            return forms
    except (FormListParseError, ApiAccessError):
        pass

    # 2) Fallback ids
    form_ids = await _fetch_form_ids(session)
    return [SurveyCTOForm(form_id=fid, title=fid, version="") for fid in form_ids]


async def fetch_submissions_wide_json(
    session_token: str,
    form_id: str,
    since_dt: datetime | None,
) -> list[dict]:
    """
    Fetch SurveyCTO wide JSON submissions:
      /api/v2/forms/data/wide/json/{FORM_ID}?date={epoch_seconds}

    SurveyCTO may respond with HTTP 417 and a server-provided wait time.
    We fail fast with a SubmissionsRateLimitError so the app can store a
    cooldown and avoid hammering the API.
    """
    session = get_session(session_token)
    date_param = _datetime_to_epoch_seconds(since_dt)

    url = f"{session.server_url}/api/v2/forms/data/wide/json/{form_id}?date={date_param}"

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
            resp = await _get_once(
                client,
                url,
                auth=(session.username, session.password),
                headers={"User-Agent": "SurveySync Connect", "Accept": "application/json"},
            )
    except httpx.RequestError as exc:
        raise SubmissionsFetchError("Unable to reach the SurveyCTO server for submissions.") from exc

    if resp.status_code in {401, 403}:
        raise AuthenticationError("SurveyCTO credentials are invalid or access is denied.")

    # 417 = cooldown rate limiting
    if resp.status_code == 417:
        wait_s = _extract_retry_after_seconds(resp)
        snippet = (resp.text or "")[:400]
        if wait_s:
            raise SubmissionsRateLimitError(
                f"SurveyCTO rate-limited. Retry after {wait_s} seconds. snippet={snippet!r}",
                retry_after_seconds=wait_s,
            )
        raise SubmissionsRateLimitError(
            f"SurveyCTO rate-limited (417). Please retry later. snippet={snippet!r}",
            retry_after_seconds=None,
        )

    # Still treat 412 as a hard error (usually constraints / API requirements)
    if resp.status_code == 412:
        snippet = (resp.text or "")[:400]
        raise SubmissionsFetchError(
            f"SurveyCTO submissions request failed with status {resp.status_code}. snippet={snippet!r}"
        )

    if resp.status_code >= 400:
        snippet = (resp.text or "")[:400]
        raise SubmissionsFetchError(
            f"SurveyCTO submissions request failed with status {resp.status_code}. snippet={snippet!r}"
        )

    # Guard JSON
    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" not in ctype:
        snippet = (resp.text or "")[:400]
        raise SubmissionsFetchError(
            f"SurveyCTO submissions did not return JSON. content-type={ctype!r} snippet={snippet!r}"
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        snippet = (resp.text or "")[:400]
        raise SubmissionsFetchError(f"SurveyCTO submissions invalid JSON. snippet={snippet!r}") from exc

    if not isinstance(payload, list):
        raise SubmissionsFetchError(f"SurveyCTO submissions unexpected JSON type: {type(payload).__name__}")

    # ensure dict rows
    rows: list[dict] = []
    for item in payload:
        if isinstance(item, dict):
            rows.append(item)
        else:
            rows.append({"_value": item})
    return rows
