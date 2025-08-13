from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Data paths
    symbols_db_path: str = "data/symbols.db"
    fx_db_path: str = "data/fx.db"
    cost_profiles_path: str = "quant/data/cost_profiles.yml"
    runs_dir: str = "runs"
    
    # Environment-specific overrides
    workspace_root: Optional[str] = None
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # If workspace_root is set, adjust other paths
        if self.workspace_root:
            root_path = Path(self.workspace_root)
            self.symbols_db_path = str(root_path / "data/symbols.db")
            self.fx_db_path = str(root_path / "data/fx.db")
            self.cost_profiles_path = str(root_path / "quant/data/cost_profiles.yml")
            self.runs_dir = str(root_path / "runs")


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]