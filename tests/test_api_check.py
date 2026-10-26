import pytest
from httpx import AsyncClient

ALGORITHMS = ["fixed_window", "sliding_window_log", "token_bucket", "gcra"]


@pytest.mark.parametrize("algorithm", ALGORITHMS)
async def test_check_allows_then_rejects(app_client: AsyncClient, algorithm: str) -> None:
    payload = {"key": f"api-{algorithm}", "algorithm": algorithm, "limit": 2, "window": 60}

    resp1 = await app_client.post("/check", json=payload)
    assert resp1.status_code == 200
    body1 = resp1.json()
    assert body1["allowed"] is True
    assert resp1.headers["x-ratelimit-limit"] == "2"
    assert "x-ratelimit-remaining" in resp1.headers
    assert "x-ratelimit-reset" in resp1.headers

    resp2 = await app_client.post("/check", json=payload)
    assert resp2.status_code == 200

    resp3 = await app_client.post("/check", json=payload)
    assert resp3.status_code == 429
    assert resp3.json()["allowed"] is False
    assert "retry-after" in resp3.headers


async def test_check_requires_config(app_client: AsyncClient) -> None:
    resp = await app_client.post("/check", json={"key": "k"})
    assert resp.status_code == 422


async def test_check_rejects_policy_and_inline_together(app_client: AsyncClient) -> None:
    resp = await app_client.post(
        "/check",
        json={"key": "k", "policy": "p", "algorithm": "gcra", "limit": 1, "window": 1},
    )
    assert resp.status_code == 422


async def test_check_unknown_policy_404(app_client: AsyncClient) -> None:
    resp = await app_client.post("/check", json={"key": "k", "policy": "does-not-exist"})
    assert resp.status_code == 404


async def test_check_with_policy(app_client: AsyncClient) -> None:
    create_resp = await app_client.post(
        "/policies",
        json={"name": "policy-check-test", "algorithm": "token_bucket", "limit": 3, "window": 10},
        headers={"Idempotency-Key": "policy-check-test-create"},
    )
    assert create_resp.status_code == 201

    resp = await app_client.post("/check", json={"key": "k-policy", "policy": "policy-check-test"})
    assert resp.status_code == 200
    assert resp.json()["algorithm"] == "token_bucket"
