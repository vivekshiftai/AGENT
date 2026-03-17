"""Service for retrieving secrets from Azure Key Vault (SAP Datasphere tokens)."""
import logging
from datetime import datetime
from typing import Optional, Tuple

from src.core.config import settings

logger = logging.getLogger(__name__)

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    DefaultAzureCredential = None
    SecretClient = None
    logger.warning(
        "Azure Key Vault libraries not available. Install: pip install azure-identity azure-keyvault-secrets"
    )


class KeyVaultService:
    """Retrieves SAP Datasphere OAuth tokens from Azure Key Vault."""

    def __init__(self):
        self._client = None
        self._vault_url = settings.azure_key_vault_url if settings else None
        if not AZURE_AVAILABLE or not self._vault_url:
            logger.warning("KeyVaultService not configured")

    def _get_client(self) -> "SecretClient":
        if not AZURE_AVAILABLE:
            raise ValueError("Azure Key Vault libraries not installed")
        if not self._vault_url:
            raise ValueError("AZURE_KEY_VAULT_URL not configured")
        if self._client is None:
            credential = DefaultAzureCredential()
            self._client = SecretClient(vault_url=self._vault_url, credential=credential)
        return self._client

    def get_user_token(self, user_id: str) -> Optional[str]:
        token_with_metadata = self.get_user_token_with_metadata(user_id)
        return token_with_metadata[0] if token_with_metadata else None

    def get_user_token_with_metadata(self, user_id: str) -> Optional[Tuple[str, Optional[datetime]]]:
        try:
            client = self._get_client()
            secret = client.get_secret(f"sap-datasphere-token-{user_id}")
            if secret and secret.value:
                created_on = (
                    getattr(secret.properties, "created_on", None)
                    if hasattr(secret, "properties")
                    else None
                )
                return (secret.value, created_on)
            return None
        except Exception as e:
            if "SecretNotFound" in str(e) or "NotFound" in str(e):
                return None
            raise

    def get_user_refresh_token(self, user_id: str) -> Optional[str]:
        return self.get_secret(f"sap-datasphere-refresh-token-{user_id}")

    def update_user_token(
        self, user_id: str, access_token: str, refresh_token: Optional[str] = None
    ) -> None:
        if not access_token:
            raise ValueError("Access token cannot be empty")
        client = self._get_client()
        client.set_secret(f"sap-datasphere-token-{user_id}", access_token)
        if refresh_token:
            client.set_secret(f"sap-datasphere-refresh-token-{user_id}", refresh_token)

    def get_secret(self, secret_name: str) -> Optional[str]:
        try:
            secret = self._get_client().get_secret(secret_name)
            return secret.value if secret and secret.value else None
        except Exception as e:
            if "SecretNotFound" in str(e) or "NotFound" in str(e):
                return None
            raise


_key_vault_service: Optional[KeyVaultService] = None


def get_key_vault_service() -> KeyVaultService:
    global _key_vault_service
    if _key_vault_service is None:
        _key_vault_service = KeyVaultService()
    return _key_vault_service
