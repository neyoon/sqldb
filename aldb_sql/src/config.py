import os
import yaml

from typing import Any
from dotenv import load_dotenv
load_dotenv(override=True)

class Config:
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        #如果更改config文件和config.yaml文件位置，需要修改这里
        project_root = os.path.abspath(os.path.join(current_dir, ".."))
        config_path = os.path.join(project_root, "config.yaml")

        try:
            with open(config_path, "r") as f:
                self.config_dict = yaml.safe_load(f) or {}
        except FileNotFoundError:
            self.config_dict = {}
        
    def get(self, key: str, env_var: str | None = None, default: Any = None) -> Any:
        if env_var:
            value = os.getenv(env_var)
            if value is not None:
                return value

        value = self.config_dict.get(key)
        if value is not None:
            return value

        return default

CONFIG = Config()