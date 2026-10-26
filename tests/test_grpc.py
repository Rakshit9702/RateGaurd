import grpc
import pytest
from httpx import AsyncClient

from app.grpc_server import GRPC_PORT
from proto import rls_pb2, rls_pb2_grpc


async def _register_policy(app_client: AsyncClient, name: str, **overrides) -> None:
    payload = {"name": name, "algorithm": "fixed_window", "limit": 2, "window": 60, **overrides}
    resp = await app_client.post("/policies", json=payload, headers={"Idempotency-Key": f"grpc-test-{name}"})
    assert resp.status_code == 201


async def test_should_rate_limit_ok_then_over_limit(app_client: AsyncClient) -> None:
    await _register_policy(app_client, "grpc-test-policy")

    async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = rls_pb2_grpc.RateLimitServiceStub(channel)
        request = rls_pb2.RateLimitRequest(
            domain="grpc-test-policy",
            descriptors=[rls_pb2.RateLimitDescriptor(entries=[rls_pb2.RateLimitDescriptor.Entry(key="user", value="alice")])],
        )

        first = await stub.ShouldRateLimit(request)
        second = await stub.ShouldRateLimit(request)
        third = await stub.ShouldRateLimit(request)

    assert first.overall_code == rls_pb2.RateLimitResponse.OK
    assert first.statuses[0].limit_remaining == 1
    assert second.overall_code == rls_pb2.RateLimitResponse.OK
    assert second.statuses[0].limit_remaining == 0
    assert third.overall_code == rls_pb2.RateLimitResponse.OVER_LIMIT
    assert third.statuses[0].duration_until_reset.seconds > 0


async def test_multiple_descriptors_over_limit_if_any_over(app_client: AsyncClient) -> None:
    await _register_policy(app_client, "grpc-multi-descriptor")

    async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = rls_pb2_grpc.RateLimitServiceStub(channel)
        entries_a = [rls_pb2.RateLimitDescriptor.Entry(key="user", value="a")]
        entries_b = [rls_pb2.RateLimitDescriptor.Entry(key="user", value="b")]
        request = rls_pb2.RateLimitRequest(
            domain="grpc-multi-descriptor",
            descriptors=[rls_pb2.RateLimitDescriptor(entries=entries_a), rls_pb2.RateLimitDescriptor(entries=entries_b)],
        )

        # Drain descriptor "a" only, by calling with just that descriptor twice.
        single_a = rls_pb2.RateLimitRequest(domain="grpc-multi-descriptor", descriptors=[rls_pb2.RateLimitDescriptor(entries=entries_a)])
        await stub.ShouldRateLimit(single_a)
        await stub.ShouldRateLimit(single_a)

        # Now check both together: "a" is exhausted, "b" is fresh.
        response = await stub.ShouldRateLimit(request)

    assert response.overall_code == rls_pb2.RateLimitResponse.OVER_LIMIT
    assert response.statuses[0].code == rls_pb2.RateLimitResponse.OVER_LIMIT
    assert response.statuses[1].code == rls_pb2.RateLimitResponse.OK


async def test_unknown_domain_is_not_found(app_client: AsyncClient) -> None:
    async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = rls_pb2_grpc.RateLimitServiceStub(channel)
        request = rls_pb2.RateLimitRequest(
            domain="does-not-exist",
            descriptors=[rls_pb2.RateLimitDescriptor(entries=[rls_pb2.RateLimitDescriptor.Entry(key="user", value="x")])],
        )

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await stub.ShouldRateLimit(request)

    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


async def test_empty_descriptors_is_invalid_argument(app_client: AsyncClient) -> None:
    await _register_policy(app_client, "grpc-empty-descriptors")

    async with grpc.aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
        stub = rls_pb2_grpc.RateLimitServiceStub(channel)
        request = rls_pb2.RateLimitRequest(domain="grpc-empty-descriptors", descriptors=[])

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await stub.ShouldRateLimit(request)

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
