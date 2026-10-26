from app.algorithms import fixed_window, gcra, sliding_window_log, token_bucket
from app.algorithms.base import RateLimiter

REGISTRY: dict[str, RateLimiter] = {
    "fixed_window": fixed_window.check,
    "sliding_window_log": sliding_window_log.check,
    "token_bucket": token_bucket.check,
    "gcra": gcra.check,
}
