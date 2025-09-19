"""Twitter API authentication handling."""

from __future__ import annotations

from typing import Dict
from singer_sdk.authenticators import SimpleAuthenticator


def build_auth(stream) -> SimpleAuthenticator:
    """Return SimpleAuthenticator injecting X-API-Key header."""
    api_key = stream.config.get("api_key")
    if not api_key:
        raise RuntimeError("Missing required config: api_key")
    return SimpleAuthenticator(
        stream=stream,
        auth_headers={"X-API-Key": api_key},
    )