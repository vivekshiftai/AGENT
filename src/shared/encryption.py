"""Encryption utilities for sensitive data like passwords."""
import os
import logging
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

logger = logging.getLogger(__name__)


class PasswordEncryptor:
    """Handles encryption and decryption of sensitive data like database passwords."""

    # Use environment variable for encryption key, with fallback to a derived key
    _key: Optional[bytes] = None
    _fernet: Optional[Fernet] = None

    @classmethod
    def _get_key(cls) -> bytes:
        """Get or generate the encryption key."""
        if cls._key is not None:
            return cls._key

        # Try to get key from environment variable
        env_key = os.getenv('ENCRYPTION_KEY')
        if env_key:
            # If key is provided as base64 string, decode it
            try:
                cls._key = base64.urlsafe_b64decode(env_key)
            except Exception:
                logger.warning("Invalid ENCRYPTION_KEY format, generating new key")
                cls._key = cls._generate_key()
        else:
            # Generate a key from a password-based key derivation
            # In production, you should set ENCRYPTION_KEY environment variable
            logger.warning("ENCRYPTION_KEY not set, using derived key. Set ENCRYPTION_KEY environment variable for security.")
            password = os.getenv('ENCRYPTION_PASSWORD', 'default-encryption-password-change-in-production')
            salt = b'insightforge_salt_2024'  # Fixed salt for consistency
            cls._key = cls._derive_key(password.encode(), salt)

        return cls._key

    @classmethod
    def _generate_key(cls) -> bytes:
        """Generate a new random encryption key."""
        return Fernet.generate_key()

    @classmethod
    def _derive_key(cls, password: bytes, salt: bytes) -> bytes:
        """Derive an encryption key from a password using PBKDF2."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password))

    @classmethod
    def _get_fernet(cls) -> Fernet:
        """Get or create the Fernet instance."""
        if cls._fernet is None:
            cls._fernet = Fernet(cls._get_key())
        return cls._fernet

    @classmethod
    def encrypt_password(cls, password: str) -> str:
        """
        Encrypt a password.

        Args:
            password: Plain text password

        Returns:
            Base64 encoded encrypted password
        """
        if not password:
            return ""

        try:
            fernet = cls._get_fernet()
            encrypted = fernet.encrypt(password.encode())
            return encrypted.decode()
        except Exception as e:
            logger.error(f"Failed to encrypt password: {str(e)}")
            raise

    @classmethod
    def decrypt_password(cls, encrypted_password: str) -> str:
        """
        Decrypt an encrypted password.

        Args:
            encrypted_password: Base64 encoded encrypted password

        Returns:
            Plain text password
        """
        if not encrypted_password:
            return ""

        try:
            fernet = cls._get_fernet()
            decrypted = fernet.decrypt(encrypted_password.encode())
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt password: {str(e)}")
            raise

    @classmethod
    def is_encrypted(cls, value: str) -> bool:
        """
        Check if a value appears to be encrypted (basic heuristic).

        Args:
            value: Value to check

        Returns:
            True if the value appears to be encrypted
        """
        if not value:
            return False

        try:
            # Try to decode as base64 and check if it looks like Fernet token
            decoded = base64.urlsafe_b64decode(value)
            return len(decoded) >= 73  # Fernet tokens are at least 73 bytes
        except Exception:
            return False
