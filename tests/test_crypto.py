"""Shared crypto layer: AES-GCM + Fernet round-trips, key handling, failures."""
from __future__ import annotations

import base64
import os

import pytest
from cryptography.fernet import Fernet

from etl_core.crypto import (
    AesGcmCipher,
    CryptoError,
    FernetCipher,
    make_cipher,
)


def test_fernet_round_trip():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    token = cipher.encrypt(b"hello world")
    assert isinstance(token, str)
    assert cipher.decrypt(token) == b"hello world"


def test_aes_gcm_round_trip_base64():
    key = base64.b64encode(os.urandom(32)).decode()
    cipher = make_cipher("aes-gcm", key)
    token = cipher.encrypt(b'{"a": 1}')
    assert cipher.decrypt(token) == b'{"a": 1}'


def test_aes_gcm_round_trip_hex():
    key = os.urandom(16).hex()  # AES-128
    cipher = make_cipher("aes-gcm", key, key_encoding="hex", token_encoding="hex")
    token = cipher.encrypt(b"data")
    assert all(c in "0123456789abcdef" for c in token)
    assert cipher.decrypt(token) == b"data"


def test_aes_gcm_raw_key():
    cipher = make_cipher("aes-gcm", "x" * 32, key_encoding="raw")  # 32 ascii bytes
    assert cipher.decrypt(cipher.encrypt(b"z")) == b"z"


def test_aes_gcm_with_aad():
    key = base64.b64encode(os.urandom(32)).decode()
    good = make_cipher("aes-gcm", key, aad=b"ctx-1")
    token = good.encrypt(b"secret")
    assert good.decrypt(token) == b"secret"
    wrong_aad = make_cipher("aes-gcm", key, aad=b"ctx-2")
    with pytest.raises(CryptoError):
        wrong_aad.decrypt(token)


def test_aes_gcm_wrong_key_fails():
    token = make_cipher("aes-gcm", base64.b64encode(os.urandom(32)).decode()).encrypt(b"x")
    other = make_cipher("aes-gcm", base64.b64encode(os.urandom(32)).decode())
    with pytest.raises(CryptoError, match="authentication failed"):
        other.decrypt(token)


def test_aes_gcm_tampered_token_fails():
    key = base64.b64encode(os.urandom(32)).decode()
    cipher = make_cipher("aes-gcm", key)
    raw = bytearray(base64.b64decode(cipher.encrypt(b"hello")))
    raw[-1] ^= 0x01  # flip a tag bit
    with pytest.raises(CryptoError):
        cipher.decrypt(base64.b64encode(bytes(raw)).decode())


def test_aes_gcm_bad_key_length():
    with pytest.raises(CryptoError, match="key must be"):
        AesGcmCipher(b"short")


def test_aes_gcm_token_too_short():
    cipher = make_cipher("aes-gcm", base64.b64encode(os.urandom(32)).decode())
    with pytest.raises(CryptoError, match="too short"):
        cipher.decrypt(base64.b64encode(b"tiny").decode())


def test_fernet_wrong_key_fails():
    token = make_cipher("fernet", Fernet.generate_key().decode()).encrypt(b"x")
    other = make_cipher("fernet", Fernet.generate_key().decode())
    with pytest.raises(CryptoError, match="Fernet decryption failed"):
        other.decrypt(token)


def test_fernet_invalid_key():
    with pytest.raises(CryptoError, match="invalid Fernet key"):
        FernetCipher("not-a-valid-fernet-key")


def test_unknown_algo():
    with pytest.raises(CryptoError, match="unknown algorithm"):
        make_cipher("rot13", "key")  # type: ignore[arg-type]


def test_bad_encoding_reports_clearly():
    with pytest.raises(CryptoError, match="not valid base64"):
        make_cipher("aes-gcm", "!!!not base64!!!")
