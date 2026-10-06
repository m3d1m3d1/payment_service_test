import secrets

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import Settings, get_settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: str | None = Security(api_key_header),
    settings: Settings = Depends(get_settings),
) -> None:
    if api_key is None or not secrets.compare_digest(
        api_key.encode(), settings.api_key.encode()
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")
