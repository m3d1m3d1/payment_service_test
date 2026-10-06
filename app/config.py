from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"

    # Outbox relay
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 50

    # Retry / DLQ: всего попыток обработки сообщения (первая + повторы)
    max_attempts: int = 3
    # Задержка перед n-й повторной попыткой: base * 2^(n-1) мс (2с, 4с, ...)
    retry_base_delay_ms: int = 2000

    # Эмуляция платежного шлюза
    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    processing_success_rate: float = 0.9

    webhook_timeout: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
