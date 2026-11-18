from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: SecretStr = Field(min_length=16)
    webhook_allowed_hosts: str = "webhook-receiver"
    webhook_signing_secret: SecretStr | None = Field(default=None, min_length=32)
    webhook_timeout: float = Field(default=5, gt=0)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    retry_base_seconds: float = Field(default=1, gt=0)

    @field_validator("webhook_signing_secret", mode="before")
    @classmethod
    def empty_signing_secret(cls, value):
        return None if value == "" else value

    @property
    def allowed_hosts(self) -> set[str]:
        return {
            host.strip().lower() for host in self.webhook_allowed_hosts.split(",") if host.strip()
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
