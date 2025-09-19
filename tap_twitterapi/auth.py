"""Twitter API authentication handling."""

from singer_sdk.authenticators import APIAuthenticatorBase


class TwitterAuthenticator(APIAuthenticatorBase):
    """Authenticator for Twitter API."""

    def __init__(self, stream, api_key: str) -> None:
        """Initialize the authenticator."""
        super().__init__(stream)
        self.api_key = api_key

    @property
    def auth_headers(self) -> dict:
        """Return the auth headers for Twitter API."""
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }
    