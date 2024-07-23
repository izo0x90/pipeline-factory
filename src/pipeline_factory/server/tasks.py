import logging
import time
import os

from ..pipeline.tasks import first_pipeline, next_pipeline, recover_pipeline

logger = logging.getLogger(__name__)


def test_task(test_id: str, sleep_time: int = 5):
    logger.info(f"Running test task in {os.getpid()=}, passed in {test_id=}")
    for _ in range(3):
        logger.info(f"Sleeping in test task for {sleep_time=} ...")
        logger.info('Pretend "Working" ...')
        time.sleep(sleep_time)
    logger.info(f"Done running test task in {os.getpid()=} for {test_id=}")
