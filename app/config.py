from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://rateguard:rateguard@localhost:5432/rateguard"
    # Comma-separated Redis URLs. Empty (default) = sharding disabled, every
    # existing behavior unchanged. See app/shard_router.py.
    redis_shard_urls: str = ""


settings = Settings()
