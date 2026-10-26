from httpx import AsyncClient


async def test_history_empty_for_unknown_policy(app_client: AsyncClient) -> None:
    resp = await app_client.get("/policies/never-existed/history")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_create_records_one_history_entry(app_client: AsyncClient) -> None:
    await app_client.post(
        "/policies",
        json={"name": "history-create-test", "algorithm": "gcra", "limit": 5, "window": 10},
        headers={"Idempotency-Key": "history-create-1"},
    )

    resp = await app_client.get("/policies/history-create-test/history")
    assert resp.status_code == 200
    history = resp.json()

    assert len(history) == 1
    assert history[0]["action"] == "created"
    assert history[0]["algorithm"] == "gcra"
    assert history[0]["limit"] == 5
    assert history[0]["idempotency_key"] == "history-create-1"


async def test_update_appends_a_second_entry_most_recent_first(app_client: AsyncClient) -> None:
    name = "history-update-test"

    await app_client.post(
        "/policies",
        json={"name": name, "algorithm": "fixed_window", "limit": 5, "window": 10},
        headers={"Idempotency-Key": "history-update-1"},
    )
    await app_client.post(
        "/policies",
        json={"name": name, "algorithm": "gcra", "limit": 20, "window": 60},
        headers={"Idempotency-Key": "history-update-2"},
    )

    resp = await app_client.get(f"/policies/{name}/history")
    history = resp.json()

    assert len(history) == 2
    # Most recent first: the update, then the original create.
    assert history[0]["action"] == "updated"
    assert history[0]["algorithm"] == "gcra"
    assert history[0]["limit"] == 20
    assert history[0]["idempotency_key"] == "history-update-2"
    assert history[1]["action"] == "created"
    assert history[1]["algorithm"] == "fixed_window"
    assert history[1]["idempotency_key"] == "history-update-1"


async def test_idempotent_replay_does_not_append_a_new_history_entry(app_client: AsyncClient) -> None:
    name = "history-replay-test"
    payload = {"name": name, "algorithm": "token_bucket", "limit": 5, "window": 10}
    headers = {"Idempotency-Key": "history-replay-key"}

    await app_client.post("/policies", json=payload, headers=headers)
    await app_client.post("/policies", json=payload, headers=headers)  # cached replay, no new DB write

    resp = await app_client.get(f"/policies/{name}/history")
    history = resp.json()

    assert len(history) == 1


async def test_history_limit_query_param(app_client: AsyncClient) -> None:
    name = "history-limit-test"
    for limit in (5, 10, 15, 20):
        await app_client.post(
            "/policies",
            json={"name": name, "algorithm": "gcra", "limit": limit, "window": 10},
            headers={"Idempotency-Key": f"history-limit-{limit}"},
        )

    resp = await app_client.get(f"/policies/{name}/history", params={"limit": 2})
    history = resp.json()

    assert len(history) == 2
    assert history[0]["limit"] == 20
    assert history[1]["limit"] == 15
