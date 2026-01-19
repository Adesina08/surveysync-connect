from __future__ import annotations

"""SurveyCTO routes.

Important: the frontend expects the "classic" response shape for forms:
  - form_id
  - title
  - version

If we ever add extra metadata (responses, fields, etc), do it behind separate
endpoints so the list-forms call stays fast and the UI can render "Unknown"
instead of "0".
"""

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from app.services import surveycto_service

router = APIRouter(prefix="/surveycto", tags=["surveycto"])


class SurveyCTOFormResponse(BaseModel):
    form_id: str
    title: str
    version: str


@router.get("/forms", response_model=list[SurveyCTOFormResponse])
async def list_forms(
    session_token: str = Query(..., description="Session token from /sessions"),
) -> list[SurveyCTOFormResponse]:
    try:
        forms = await surveycto_service.list_forms(session_token)
    except surveycto_service.InvalidSessionError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    except surveycto_service.AuthenticationError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    except surveycto_service.ApiAccessError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except surveycto_service.ServerConnectionError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    except surveycto_service.FormListParseError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    return [
        SurveyCTOFormResponse(
            form_id=f.form_id,
            title=f.title,
            version=f.version or "1",
        )
        for f in forms
    ]
