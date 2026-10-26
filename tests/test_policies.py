from httpx import AsyncClient


async def test_create_and_get_policy(app_client: AsyncClient) -> None:
    resp = await app_client.post(
        "/policies",
        json={"name": "test-policy-1", "algorithm": "gcra", "limit": 5, "window": 10},
        headers={"Idempotency-Key": "create-test-policy-1"},
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "test-policy-1"

    get_resp = await app_client.get("/policies/test-policy-1")
    assert get_resp.status_code == 200
    assert get_resp.json()["algorithm"] == "gcra"


async def test_get_missing_policy(app_client: AsyncClient) -> None:
    resp = await app_client.get("/policies/does-not-exist")
    assert resp.status_code == 404


async def test_create_policy_requires_idempotency_key(app_client: AsyncClient) -> None:
    resp = await app_client.post(
        "/policies", json={"name": "no-header-policy", "algorithm": "gcra", "limit": 5, "window": 10}
    )
    assert resp.status_code == 422


async def test_idempotent_replay_returns_cached_response(app_client: AsyncClient) -> None:
    payload = {"name": "idem-policy", "algorithm": "token_bucket", "limit": 5, "window": 10}
    headers = {"Idempotency-Key": "idem-key-1"}

    resp1 = await app_client.post("/policies", json=payload, headers=headers)
    resp2 = await app_client.post("/policies", json=payload, headers=headers)

    assert resp1.status_code == 201
    assert resp2.status_code == 201
    assert resp1.json() == resp2.json()


async def test_reregistering_policy_name_overwrites(app_client: AsyncClient) -> None:
    name = "overwrite-policy"

    resp1 = await app_client.post(
        "/policies",
        json={"name": name, "algorithm": "fixed_window", "limit": 5, "window": 10},
        headers={"Idempotency-Key": "overwrite-key-1"},
    )
    assert resp1.status_code == 201

    resp2 = await app_client.post(
        "/policies",
        json={"name": name, "algorithm": "gcra", "limit": 20, "window": 60},
        headers={"Idempotency-Key": "overwrite-key-2"},
    )
    assert resp2.status_code == 201

    get_resp = await app_client.get(f"/policies/{name}")
    assert get_resp.json() == {"name": name, "algorithm": "gcra", "limit": 20, "window": 60, "burst": None}


async def test_idempotency_conflict_on_different_body(app_client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-key-conflict"}

    resp1 = await app_client.post(
        "/policies",
        json={"name": "conflict-policy-a", "algorithm": "gcra", "limit": 5, "window": 10},
        headers=headers,
    )
    assert resp1.status_code == 201

    resp2 = await app_client.post(
        "/policies",
        json={"name": "conflict-policy-b", "algorithm": "gcra", "limit": 5, "window": 10},
        headers=headers,
    )
    assert resp2.status_code == 409
