from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from app.services import surveycto_service

router = APIRouter(prefix="/surveycto", tags=["surveycto"])


class SurveyCTOField(BaseModel):
    name: str
    type: str
    label: str
    isPrimaryKey: bool = False


class SurveyCTOFormResponse(BaseModel):
    id: str
    name: str
    version: str
    responses: int = -1
    lastUpdated: str = "Unknown"
    fields: list[SurveyCTOField] = []


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

    # We don't yet fetch real response counts/fields here (avoid heavy calls).
    return [
        SurveyCTOFormResponse(
            id=f.form_id,
            name=f.title,
            version=f.version or "1",
            responses=-1,
            lastUpdated="Unknown",
            fields=[],
        )
        for f in forms
    ]
