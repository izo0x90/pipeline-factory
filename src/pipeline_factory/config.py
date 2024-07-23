from dataclasses import dataclass
import pathlib


@dataclass
class AppConfig:
    TASK_SCHEDULE_FILE: pathlib.Path = pathlib.Path("schedule.json")
    OUTPUT_DIR: pathlib.Path = pathlib.Path("output")


_config: AppConfig


def register_config(config: AppConfig) -> None:
    global _config
    _config = config


def get_config() -> AppConfig:
    return _config
