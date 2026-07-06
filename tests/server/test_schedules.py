"""Schedule CRUD, cron/timezone validation, next_run recomputation, isolation."""
from __future__ import annotations


async def test_create_sets_next_run(auth_client, make_pipeline):
    pid = await make_pipeline()
    r = await auth_client.post(
        "/schedules",
        json={"pipeline_id": pid, "cron_expr": "*/5 * * * *", "timezone": "America/New_York"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["next_run"] is not None
    assert body["enabled"] is True


async def test_invalid_cron_and_timezone_rejected(auth_client, make_pipeline):
    pid = await make_pipeline()
    bad_cron = await auth_client.post(
        "/schedules", json={"pipeline_id": pid, "cron_expr": "not a cron", "timezone": "UTC"}
    )
    assert bad_cron.status_code == 422
    bad_tz = await auth_client.post(
        "/schedules", json={"pipeline_id": pid, "cron_expr": "* * * * *", "timezone": "Mars/Base"}
    )
    assert bad_tz.status_code == 422


async def test_schedule_for_unowned_pipeline_404(auth_client, make_pipeline, other_headers):
    pid = await make_pipeline()
    r = await auth_client.post(
        "/schedules",
        headers=other_headers,
        json={"pipeline_id": pid, "cron_expr": "* * * * *", "timezone": "UTC"},
    )
    assert r.status_code == 404


async def test_update_recomputes_next_run(auth_client, make_pipeline):
    pid = await make_pipeline()
    created = (
        await auth_client.post(
            "/schedules", json={"pipeline_id": pid, "cron_expr": "0 0 1 1 *", "timezone": "UTC"}
        )
    ).json()
    sid, first_next = created["id"], created["next_run"]
    updated = await auth_client.put(f"/schedules/{sid}", json={"cron_expr": "*/1 * * * *"})
    assert updated.status_code == 200
    assert updated.json()["next_run"] != first_next  # recomputed for the new cron


async def test_disable_and_delete(auth_client, make_pipeline):
    pid = await make_pipeline()
    sid = (
        await auth_client.post(
            "/schedules", json={"pipeline_id": pid, "cron_expr": "* * * * *", "timezone": "UTC"}
        )
    ).json()["id"]
    disabled = await auth_client.put(f"/schedules/{sid}", json={"enabled": False})
    assert disabled.json()["enabled"] is False
    assert (await auth_client.delete(f"/schedules/{sid}")).status_code == 204
    assert (await auth_client.get(f"/schedules/{sid}")).status_code == 404


async def test_schedule_isolation(auth_client, make_pipeline, other_headers):
    pid = await make_pipeline()
    sid = (
        await auth_client.post(
            "/schedules", json={"pipeline_id": pid, "cron_expr": "* * * * *", "timezone": "UTC"}
        )
    ).json()["id"]
    assert (await auth_client.get(f"/schedules/{sid}", headers=other_headers)).status_code == 404
    assert (await auth_client.get("/schedules", headers=other_headers)).json() == []
