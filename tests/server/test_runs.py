"""Run trigger + execution: success, structured failures, secrets, SSE."""
from __future__ import annotations

import base64
import os

from cryptography.fernet import Fernet

from etl_core.crypto import make_cipher

FAIL_SPEC = {
    "pipeline_id": "fail",
    "nodes": [
        {"id": "gen", "type": "iterator", "config": {"mode": "array", "array": [1]}},
        {
            "id": "boom",
            "type": "transform",
            "config": {"ops": [{"op": "computed", "target": "x", "expression": "value / 0"}]},
        },
    ],
    "edges": [{"from": "gen", "to": "boom"}],
}


def _decrypt_spec() -> tuple[dict, str, str]:
    key = Fernet.generate_key().decode()
    token = make_cipher("fernet", key).encrypt(b"top-secret-payload")
    spec = {
        "pipeline_id": "dec",
        "nodes": [
            {"id": "gen", "type": "iterator", "config": {"mode": "array", "array": [token]}},
            {
                "id": "dec",
                "type": "decrypt",
                "config": {"algo": "fernet", "secret_ref": "RUNKEY", "fields": ["value"]},
            },
        ],
        "edges": [{"from": "gen", "to": "dec"}],
    }
    return spec, key, token


async def test_trigger_runs_inline_and_succeeds(auth_client, make_pipeline):
    pid = await make_pipeline()
    r = await auth_client.post(f"/pipelines/{pid}/runs", json={})
    assert r.status_code == 202, r.text
    assert r.json()["status"] == "succeeded"  # inline queue ran it to completion
    assert r.json()["trigger"] == "manual"


async def test_run_detail_has_logs_and_result(auth_client, make_pipeline):
    pid = await make_pipeline()
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    detail = (await auth_client.get(f"/runs/{rid}")).json()
    assert detail["status"] == "succeeded"
    assert len(detail["logs"]) > 0
    assert detail["errors"] == []
    doubled = [rec["doubled"] for rec in detail["result"]["outputs"]["double"]]
    assert doubled == [2, 4, 6]


async def test_list_runs_filter_by_pipeline(auth_client, make_pipeline):
    pid = await make_pipeline()
    await auth_client.post(f"/pipelines/{pid}/runs", json={})
    await auth_client.post(f"/pipelines/{pid}/runs", json={})
    runs = (await auth_client.get("/runs", params={"pipeline_id": pid})).json()
    assert len(runs) == 2
    assert all(run["pipeline_id"] == pid for run in runs)


async def test_failed_run_persists_structured_error(auth_client, make_pipeline):
    pid = await make_pipeline(spec=FAIL_SPEC, name="fails")
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    detail = (await auth_client.get(f"/runs/{rid}")).json()
    assert detail["status"] == "failed"
    assert detail["error_count"] >= 1
    categories = {e["category"] for e in detail["errors"]}
    assert "transform" in categories
    assert detail["errors"][0]["node_id"] == "boom"


async def test_run_uses_encrypted_secret(auth_client, make_pipeline):
    spec, key, _token = _decrypt_spec()
    await auth_client.post("/secrets", json={"ref": "RUNKEY", "value": key})
    pid = await make_pipeline(spec=spec, name="dec")
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    detail = (await auth_client.get(f"/runs/{rid}")).json()
    assert detail["status"] == "succeeded", detail["errors"]
    assert detail["result"]["outputs"]["dec"][0]["value"] == "top-secret-payload"


async def test_run_missing_secret_fails_with_config_error(auth_client, make_pipeline):
    spec, _key, _token = _decrypt_spec()  # do NOT store RUNKEY
    pid = await make_pipeline(spec=spec, name="dec-missing")
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    detail = (await auth_client.get(f"/runs/{rid}")).json()
    assert detail["status"] == "failed"
    assert detail["errors"][0]["category"] == "config"
    assert "RUNKEY" in detail["errors"][0]["message"]


async def test_run_isolation(auth_client, make_pipeline, other_headers):
    pid = await make_pipeline()
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    assert (await auth_client.get(f"/runs/{rid}", headers=other_headers)).status_code == 404
    # and another user cannot trigger a run on someone else's pipeline
    assert (
        await auth_client.post(f"/pipelines/{pid}/runs", headers=other_headers, json={})
    ).status_code == 404


async def test_sse_streams_events(auth_client, make_pipeline):
    pid = await make_pipeline()
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    events: list[str] = []
    async with auth_client.stream("GET", f"/runs/{rid}/events") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        async for line in resp.aiter_lines():
            if line.startswith("event:"):
                events.append(line.split(":", 1)[1].strip())
            if "done" in events:
                break
    assert "log" in events
    assert "status" in events
    assert "done" in events


async def test_aes_gcm_secret_run(auth_client, make_pipeline):
    # exercise the aes-gcm branch of the crypto layer through a run
    key = base64.b64encode(os.urandom(32)).decode()
    token = make_cipher("aes-gcm", key).encrypt(b"aes-payload")
    spec = {
        "pipeline_id": "aes",
        "nodes": [
            {"id": "gen", "type": "iterator", "config": {"mode": "array", "array": [token]}},
            {"id": "dec", "type": "decrypt",
             "config": {"algo": "aes-gcm", "secret_ref": "AESKEY", "fields": ["value"]}},
        ],
        "edges": [{"from": "gen", "to": "dec"}],
    }
    await auth_client.post("/secrets", json={"ref": "AESKEY", "value": key})
    pid = await make_pipeline(spec=spec, name="aes")
    rid = (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    detail = (await auth_client.get(f"/runs/{rid}")).json()
    assert detail["status"] == "succeeded", detail["errors"]
    assert detail["result"]["outputs"]["dec"][0]["value"] == "aes-payload"
