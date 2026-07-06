"""Password hashing (bcrypt) and JWT access tokens (HS256 via PyJWT).

Passwords are SHA-256 pre-hashed before bcrypt so arbitrary-length inputs are
supported (bcrypt otherwise silently truncates at 72 bytes) and embedded NUL
bytes can't truncate the hash. Tokens carry the user id in ``sub`` and an
``exp`` expiry.
"""
from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt


def _prehash(password: str) -> bytes:
    return base64.b64encode(hashlib.sha256(password.encode("utf-8")).digest())


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_prehash(password), bcrypt.gensalt()).decode("ascii")


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prehash(password), hashed.encode("ascii"))
    except (ValueError, TypeError):
        return False


class TokenError(Exception):
    """A JWT was missing, malformed, or expired."""


def create_access_token(
    subject: str, *, secret: str, algorithm: str = "HS256", ttl_minutes: int = 60 * 24
) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ttl_minutes)).timestamp()),
    }
    return jwt.encode(payload, secret, algorithm=algorithm)


def decode_token(token: str, *, secret: str, algorithms: list[str] | None = None) -> dict:
    try:
        return jwt.decode(token, secret, algorithms=algorithms or ["HS256"])
    except jwt.PyJWTError as exc:
        raise TokenError(str(exc)) from exc
