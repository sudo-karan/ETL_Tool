"""decrypt node: AES-GCM / Fernet field decryption, outputs, errors, redaction."""
from __future__ import annotations

import base64
import json
import os

from cryptography.fernet import Fernet

from conftest import run_pipeline
from etl_core.crypto import make_cipher
from etl_core.engine import RunStatus


def decrypt_pipeline(records, decrypt_config):
    return {
        "pipeline_id": "dec",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {"id": "dec", "type": "decrypt", "config": decrypt_config},
        ],
        "edges": [{"from": "src", "to": "dec"}],
    }


async def run_decrypt(records, config, secrets):
    return await run_pipeline(decrypt_pipeline(records, config), secrets)


async def test_fernet_decrypts_fields():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    records = [{"id": 1, "ssn": cipher.encrypt(b"111-22-3333")}]
    result = await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["ssn"]},
        {"K": key},
    )
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert result.outputs["dec"] == [{"id": 1, "ssn": "111-22-3333"}]


async def test_aes_gcm_decrypts_multiple_fields():
    key = base64.b64encode(os.urandom(32)).decode()
    cipher = make_cipher("aes-gcm", key)
    records = [
        {"a": cipher.encrypt(b"one"), "b": cipher.encrypt(b"two"), "keep": 9},
    ]
    result = await run_decrypt(
        records,
        {"algo": "aes-gcm", "secret_ref": "K", "fields": ["a", "b"]},
        {"K": key},
    )
    assert result.outputs["dec"] == [{"a": "one", "b": "two", "keep": 9}]


async def test_nested_field_path():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    records = [{"user": {"secret": cipher.encrypt(b"hidden")}}]
    result = await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["user.secret"]},
        {"K": key},
    )
    assert result.outputs["dec"] == [{"user": {"secret": "hidden"}}]


async def test_output_json_parses_plaintext():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    records = [{"blob": cipher.encrypt(b'{"x": [1, 2]}')}]
    result = await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["blob"], "output": "json"},
        {"K": key},
    )
    assert result.outputs["dec"] == [{"blob": {"x": [1, 2]}}]


async def test_output_bytes_base64():
    key = base64.b64encode(os.urandom(32)).decode()
    cipher = make_cipher("aes-gcm", key)
    payload = bytes([0, 255, 10, 200])
    records = [{"raw": cipher.encrypt(payload)}]
    result = await run_decrypt(
        records,
        {"algo": "aes-gcm", "secret_ref": "K", "fields": ["raw"], "output": "bytes_base64"},
        {"K": key},
    )
    assert base64.b64decode(result.outputs["dec"][0]["raw"]) == payload


async def test_does_not_mutate_input_records():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    original = {"user": {"secret": cipher.encrypt(b"hidden")}}
    records = [original]
    await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["user.secret"]},
        {"K": key},
    )
    assert original["user"]["secret"] != "hidden"  # input untouched (deep-copied)


async def test_wrong_key_is_decryption_error():
    key = Fernet.generate_key().decode()
    other = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    records = [{"ssn": cipher.encrypt(b"x")}]
    result = await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["ssn"]},
        {"K": other},
    )
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "decryption"


async def test_missing_field_error_vs_skip():
    key = Fernet.generate_key().decode()
    records = [{"other": 1}]
    err = await run_decrypt(
        records, {"algo": "fernet", "secret_ref": "K", "fields": ["ssn"]}, {"K": key}
    )
    assert err.status == RunStatus.FAILED
    assert err.errors[0].category.value == "decryption"

    skipped = await run_decrypt(
        records,
        {"algo": "fernet", "secret_ref": "K", "fields": ["ssn"], "on_missing": "skip"},
        {"K": key},
    )
    assert skipped.status == RunStatus.SUCCEEDED
    assert skipped.outputs["dec"] == [{"other": 1}]


async def test_non_string_token_is_decryption_error():
    key = Fernet.generate_key().decode()
    result = await run_decrypt(
        records=[{"ssn": 12345}],
        config={"algo": "fernet", "secret_ref": "K", "fields": ["ssn"]},
        secrets={"K": key},
    )
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "decryption"
    assert "not a string token" in result.errors[0].message


async def test_missing_secret_is_config_error():
    result = await run_decrypt(
        records=[{"ssn": "x"}],
        config={"algo": "fernet", "secret_ref": "NOPE", "fields": ["ssn"]},
        secrets={},
    )
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"


async def test_key_never_leaks_into_output():
    key = Fernet.generate_key().decode()
    cipher = make_cipher("fernet", key)
    records = [{"ssn": cipher.encrypt(b"x")}]
    result = await run_decrypt(
        records, {"algo": "fernet", "secret_ref": "K", "fields": ["ssn"]}, {"K": key}
    )
    dumped = result.model_dump_json()
    assert key not in dumped
