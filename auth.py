from typing import Optional

from fastapi import Header, HTTPException, status

from config import settings
from models import IngestionIdentity


async def authenticate_request(
    x_project_name: Optional[str] = Header(default=None, alias="x-project-name"),
    x_ingest_key: Optional[str] = Header(default=None, alias="x-ingest-key"),
) -> IngestionIdentity:
    if not x_project_name:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing x-project-name header",
        )

    if x_ingest_key != settings.INGEST_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid ingest key",
        )

    return IngestionIdentity(project_name=x_project_name)