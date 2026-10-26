import asyncpg
import grpc
import redis.asyncio as redis
from google.protobuf.duration_pb2 import Duration

from app.algorithms import REGISTRY
from app.policies import get_policy
from proto import rls_pb2, rls_pb2_grpc

GRPC_PORT = 50051


def _descriptor_key(descriptor: rls_pb2.RateLimitDescriptor) -> str:
    """Same idea as the HTTP API's `key`, built from Envoy's generic
    entries -- sorted so entry order in the request never changes identity.
    """
    entries = sorted((entry.key, entry.value) for entry in descriptor.entries)
    return "|".join(f"{k}={v}" for k, v in entries)


class RateLimitServicer(rls_pb2_grpc.RateLimitServiceServicer):
    """Implements Envoy's RateLimitService.ShouldRateLimit RPC on top of the
    same policy registry and algorithm registry POST /check uses -- `domain`
    resolves to a named policy exactly like `/check` with `{"policy": ...}`.
    """

    def __init__(self, redis_client: redis.Redis, db_pool: asyncpg.Pool) -> None:
        self._redis = redis_client
        self._db = db_pool

    async def ShouldRateLimit(
        self, request: rls_pb2.RateLimitRequest, context: grpc.aio.ServicerContext
    ) -> rls_pb2.RateLimitResponse:
        if not request.descriptors:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "at least one descriptor is required")

        policy = await get_policy(self._db, request.domain)
        if policy is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"unknown domain '{request.domain}' (no such policy)")

        check_fn = REGISTRY[policy.algorithm]

        overall_code = rls_pb2.RateLimitResponse.OK
        statuses = []
        for descriptor in request.descriptors:
            key = _descriptor_key(descriptor)
            decision = await check_fn(self._redis, key, policy.limit, policy.window, policy.burst)

            code = rls_pb2.RateLimitResponse.OK if decision.allowed else rls_pb2.RateLimitResponse.OVER_LIMIT
            if not decision.allowed:
                overall_code = rls_pb2.RateLimitResponse.OVER_LIMIT

            duration = Duration()
            duration.FromSeconds(decision.retry_after)
            statuses.append(
                rls_pb2.RateLimitResponse.DescriptorStatus(
                    code=code, limit_remaining=decision.remaining, duration_until_reset=duration
                )
            )

        return rls_pb2.RateLimitResponse(overall_code=overall_code, statuses=statuses)


async def start_grpc_server(redis_client: redis.Redis, db_pool: asyncpg.Pool, port: int = GRPC_PORT) -> grpc.aio.Server:
    server = grpc.aio.server()
    rls_pb2_grpc.add_RateLimitServiceServicer_to_server(RateLimitServicer(redis_client, db_pool), server)
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    return server


async def stop_grpc_server(server: grpc.aio.Server) -> None:
    await server.stop(grace=2)
