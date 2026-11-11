from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongo_uri: str
    mongo_database: str
    redis_url: str

    # Prefixo das chaves usadas no Redis (fila, retries, dead-letter, processing)
    event_queue_prefix: str = "events"

    # Worker
    worker_concurrency: int = 4
    worker_max_attempts: int = 5
    worker_retry_base_delay: float = 2.0
    worker_retry_max_delay: float = 300.0
    worker_handler_timeout: float = 30.0
    worker_poll_timeout: float = 1.0
    worker_scheduler_interval: float = 1.0

    # Roda o worker dentro do processo da API (útil em desenvolvimento)
    run_embedded_worker: bool = False

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()  # type: ignore
