from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

Algorithm = Literal["fixed_window", "sliding_window_log", "token_bucket", "gcra"]


class CheckRequest(BaseModel):
    key: str
    policy: str | None = None
    algorithm: Algorithm | None = None
    limit: int | None = Field(default=None, gt=0)
    window: int | None = Field(default=None, gt=0)
    burst: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_config_source(self) -> "CheckRequest":
        inline_fields_set = self.algorithm is not None or self.limit is not None or self.window is not None
        if self.policy is not None and inline_fields_set:
            raise ValueError("provide either `policy` or inline algorithm/limit/window, not both")
        if self.policy is None and (self.algorithm is None or self.limit is None or self.window is None):
            raise ValueError("must provide `policy`, or all of `algorithm`, `limit`, and `window`")
        return self


class CheckResponse(BaseModel):
    allowed: bool
    limit: int
    remaining: int
    reset: int
    retry_after: int
    algorithm: str
    key: str


class PolicyCreateRequest(BaseModel):
    name: str
    algorithm: Algorithm
    limit: int = Field(gt=0)
    window: int = Field(gt=0)
    burst: int | None = Field(default=None, gt=0)


class PolicyResponse(BaseModel):
    name: str
    algorithm: Algorithm
    limit: int
    window: int
    burst: int | None = None


class PolicyHistoryEntry(BaseModel):
    action: Literal["created", "updated"]
    algorithm: Algorithm
    limit: int
    window: int
    burst: int | None = None
    idempotency_key: str | None = None
    changed_at: datetime
