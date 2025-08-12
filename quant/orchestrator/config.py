from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    symbols_db_path: str = "/data/symbols.db"
    fx_db_path: str = "/data/fx.db"
    cost_profiles_path: str = "/workspace/quant/data/cost_profiles.yml"
    runs_dir: str = "/workspace/runs"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]