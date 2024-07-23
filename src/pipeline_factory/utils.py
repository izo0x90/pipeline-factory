import functools
import os
import pathlib
import logging

from . import config

logger = logging.getLogger(__name__)


def get_dir_files_glob(dir_: pathlib.Path, supported_types):
    logger.debug(f"Loading file paths for {dir_}...")

    file_paths = []
    for type_glob in supported_types:
        for file_path in dir_.glob(type_glob):
            logger.debug(file_path)
            file_paths.append(file_path)

    return file_paths


def get_env_or_fail(env_var_name: str) -> str:
    missing_env_ok = os.getenv("MISSING_ENV_OK")
    env_var_val = os.getenv(env_var_name)
    if not env_var_val:
        if missing_env_ok:
            env_var_val = ""
        else:
            raise ValueError(f"Missing config for {env_var_val}")

    return env_var_val


@functools.lru_cache
def dir_path_for_generation(id, dir_path: str):
    id = str(id)
    root_dir = pathlib.Path(config.get_config().OUTPUT_DIR) / id
    return root_dir / dir_path
