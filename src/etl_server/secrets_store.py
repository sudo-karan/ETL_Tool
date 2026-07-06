"""Encrypted-at-rest secrets, per user.

Secret values are stored as crypto-layer tokens (AES-GCM / Fernet, keyed by the
server master key) and decrypted only inside the worker at run time -- the same
:class:`~etl_core.crypto.Cipher` the ``decrypt`` node uses. :class:`DbSecretsProvider`
implements the engine's :class:`~etl_core.secrets.SecretsProvider` interface, so
the worker resolves refs with the engine's own ``resolve_secrets`` helper.
"""
from __future__ import annotations

import uuid

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.crypto import Cipher, CryptoError
from etl_core.errors import SecretNotFoundError
from etl_core.secrets import SecretsProvider

from .models import Secret


class DbSecretsProvider(SecretsProvider):
    """Resolves ``secret_ref`` -> decrypted value for one owner."""

    def __init__(self, session: AsyncSession, owner_id: uuid.UUID, cipher: Cipher):
        self._session = session
        self._owner_id = owner_id
        self._cipher = cipher

    async def get(self, ref: str) -> str:
        row = (
            await self._session.execute(
                select(Secret).where(Secret.owner_id == self._owner_id, Secret.ref == ref)
            )
        ).scalar_one_or_none()
        if row is None:
            raise SecretNotFoundError(ref)
        try:
            return self._cipher.decrypt(row.ciphertext).decode("utf-8")
        except (CryptoError, UnicodeDecodeError) as exc:
            raise SecretNotFoundError(ref) from exc


async def upsert_secret(
    session: AsyncSession, owner_id: uuid.UUID, ref: str, value: str, cipher: Cipher
) -> Secret:
    """Create or replace a secret, storing only the ciphertext."""
    token = cipher.encrypt(value.encode("utf-8"))
    row = (
        await session.execute(
            select(Secret).where(Secret.owner_id == owner_id, Secret.ref == ref)
        )
    ).scalar_one_or_none()
    if row is None:
        row = Secret(owner_id=owner_id, ref=ref, ciphertext=token)
        session.add(row)
    else:
        row.ciphertext = token
    await session.flush()
    return row


async def delete_secret(session: AsyncSession, owner_id: uuid.UUID, ref: str) -> bool:
    result = await session.execute(
        delete(Secret).where(Secret.owner_id == owner_id, Secret.ref == ref)
    )
    return result.rowcount > 0
