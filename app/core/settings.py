from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongo_uri: str
    mongo_database: str
    redis_url: str
    model_config = SettingsConfigDict(env_file=".env", env_file_enconding="utf-8")  # type: ignore


settings = Settings()  # type: ignore
