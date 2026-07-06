"""Shared symmetric crypto: field-level decryption today, secret-at-rest later.

The ``decrypt`` node (Phase 2) and the server's encrypted secrets store
(Phase 3) both need the same primitives, so they live here once. A single
module means the two agree on token layout, key handling and error surface.

Two algorithms are supported:

* ``aes-gcm`` -- AES-GCM (AEAD), key size 128/192/256 bits. A token is the
  layout ``nonce(12 bytes) || ciphertext || tag(16 bytes)``, transported as
  Base64 (default) or hex. Optional additional-authenticated-data (AAD) binds
  context to the ciphertext.
* ``fernet`` -- ``cryptography``'s Fernet (AES-128-CBC + HMAC-SHA256, with a
  timestamp). The token and key are the standard urlsafe-Base64 forms.

Everything is a pure function of its inputs -- no module-level state -- so it
is safe to use from concurrent runs.
"""
from __future__ import annotations

import base64
import binascii
import os
from abc import ABC, abstractmethod
from typing import Literal

from cryptography.exceptions import InvalidTag
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

Algo = Literal["aes-gcm", "fernet"]
KeyEncoding = Literal["base64", "hex", "raw"]
TokenEncoding = Literal["base64", "hex"]

_AESGCM_NONCE_BYTES = 12
_AESGCM_KEY_SIZES = (16, 24, 32)  # AES-128 / 192 / 256


class CryptoError(Exception):
    """Any key-parsing, encryption or decryption failure.

    The decrypt node converts this into a ``decryption``-category NodeError;
    its message never contains key or plaintext material.
    """


# --------------------------------------------------------------------------
# Encoding helpers
# --------------------------------------------------------------------------
def _decode(value: str, encoding: str, *, what: str) -> bytes:
    try:
        if encoding == "base64":
            return base64.b64decode(value, validate=True)
        if encoding == "hex":
            return bytes.fromhex(value.strip())
        return value.encode("utf-8")  # raw
    except (binascii.Error, ValueError) as exc:
        raise CryptoError(f"{what} is not valid {encoding}: {exc}") from exc


# --------------------------------------------------------------------------
# Cipher interface
# --------------------------------------------------------------------------
class Cipher(ABC):
    """Build once from key material, then apply to many values."""

    @abstractmethod
    def decrypt(self, token: str) -> bytes:
        """Return plaintext bytes for a token, or raise :class:`CryptoError`."""

    @abstractmethod
    def encrypt(self, plaintext: bytes) -> str:
        """Return a token for plaintext bytes (used by tests and Phase 3)."""


class AesGcmCipher(Cipher):
    def __init__(
        self,
        key: bytes,
        *,
        token_encoding: TokenEncoding = "base64",
        aad: bytes | None = None,
    ):
        if len(key) not in _AESGCM_KEY_SIZES:
            raise CryptoError(
                f"AES-GCM key must be {'/'.join(str(s) for s in _AESGCM_KEY_SIZES)} "
                f"bytes (got {len(key)}); check the key and its encoding"
            )
        self._aesgcm = AESGCM(key)
        self._token_encoding: TokenEncoding = token_encoding
        self._aad = aad

    def decrypt(self, token: str) -> bytes:
        raw = _decode(token, self._token_encoding, what="ciphertext token")
        if len(raw) < _AESGCM_NONCE_BYTES + 16:
            raise CryptoError(
                "AES-GCM token too short to contain a nonce, ciphertext and tag"
            )
        nonce, ciphertext = raw[:_AESGCM_NONCE_BYTES], raw[_AESGCM_NONCE_BYTES:]
        try:
            return self._aesgcm.decrypt(nonce, ciphertext, self._aad)
        except InvalidTag as exc:
            raise CryptoError(
                "AES-GCM authentication failed: wrong key/AAD or corrupted ciphertext"
            ) from exc

    def encrypt(self, plaintext: bytes) -> str:
        nonce = os.urandom(_AESGCM_NONCE_BYTES)
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, self._aad)
        raw = nonce + ciphertext
        if self._token_encoding == "hex":
            return raw.hex()
        return base64.b64encode(raw).decode("ascii")


class FernetCipher(Cipher):
    def __init__(self, key: str):
        try:
            self._fernet = Fernet(key.encode("utf-8") if isinstance(key, str) else key)
        except (ValueError, binascii.Error) as exc:
            raise CryptoError(f"invalid Fernet key: {exc}") from exc

    def decrypt(self, token: str) -> bytes:
        try:
            return self._fernet.decrypt(token.encode("utf-8"))
        except InvalidToken as exc:
            raise CryptoError(
                "Fernet decryption failed: wrong key or corrupted/expired token"
            ) from exc

    def encrypt(self, plaintext: bytes) -> str:
        return self._fernet.encrypt(plaintext).decode("ascii")


def make_cipher(
    algo: Algo,
    key_material: str,
    *,
    key_encoding: KeyEncoding = "base64",
    token_encoding: TokenEncoding = "base64",
    aad: bytes | None = None,
) -> Cipher:
    """Construct a :class:`Cipher` from a key string and algorithm options.

    ``key_material`` is the raw secret value (resolved from a ``secret_ref``).
    For ``aes-gcm`` it is decoded per ``key_encoding``; for ``fernet`` it is a
    urlsafe-Base64 Fernet key and the encoding options are ignored.
    """
    if algo == "fernet":
        return FernetCipher(key_material)
    if algo == "aes-gcm":
        key = _decode(key_material, key_encoding, what="AES-GCM key")
        return AesGcmCipher(key, token_encoding=token_encoding, aad=aad)
    raise CryptoError(f"unknown algorithm {algo!r} (expected 'aes-gcm' or 'fernet')")
