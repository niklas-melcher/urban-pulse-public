from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    telraam_api_key: str
    telraam_segment_ids: str  # comma-separated, split where needed
    luchtmeetnet_base_url: str = "https://api.luchtmeetnet.nl/open_api"
    duckdb_path: str = "./data/urban_pulse.duckdb"
    luchtmeetnet_station_numbers: str

    databricks_access_token: str | None = None
    databricks_host: str | None = None
    databricks_http_path: str | None = None

    redpanda_bootstrap_servers: str | None = None
    redpanda_username: str | None = None
    redpanda_password: str | None = None
    redpanda_topic: str | None = None
