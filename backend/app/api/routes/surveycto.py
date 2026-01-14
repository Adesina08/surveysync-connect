from __future__ import annotations

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
        # 404 from SurveyCTO means the endpoint isn't available on that server,
        # which is more like a bad gateway / misconfiguration than "forbidden".
        if getattr(exc, "status_code", None) == 404:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    except surveycto_service.ServerConnectionError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    except surveycto_service.FormListParseError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    except Exception as exc:
        # Prevents undocumented 500s and makes debugging much easier
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unexpected error while listing forms: {type(exc).__name__}",
        ) from exc

    return [
        SurveyCTOFormResponse(form_id=form.form_id, title=form.title, version=form.version)
        for form in forms
    ]
