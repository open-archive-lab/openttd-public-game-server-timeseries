import logging
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
)


class Settings(BaseSettings):
    ARCHIVE_DIR: Path = Path("./archives")
    DOWNLOAD_DIR: Path = Path("./downloads")
    FILES: dict[str, dict[str, str]] = {
        "listing.html": {"url": "https://servers.openttd.org/listing"}
    }
    STAGING_DB: Path = Path("./staging.sqlite")
    FINAL_DB: Path = Path("./series.csv")

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
