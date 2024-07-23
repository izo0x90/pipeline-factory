from enum import StrEnum
import os
import logging
from pathlib import Path
import time
from typing import OrderedDict


from .types import PipelineTypes
from pipeline_factory.pipeline.pipeline import (
    RecoverableStepType,
    register_step,
    Generation,
    STATUS_KEY,
)


logger = logging.getLogger(__name__)


class PipelineStatus(StrEnum):
    TEST_STATUS = "test status"


@register_step(PipelineTypes.EXAMPLE_1)
def step_one(generation: Generation, context: OrderedDict) -> None:
    logger.debug("Step one: (Pretend) Setting up example context/ configuration")
    context["some_example_value"] = "TEST VALUE"
    context[STATUS_KEY] = PipelineStatus.TEST_STATUS
    logger.debug(f"Step one done with, {context=}")


@register_step(PipelineTypes.EXAMPLE_1)
class StepTwoIsRecoverable(RecoverableStepType):
    @staticmethod
    def step_action(generation: Generation, context: OrderedDict) -> None:
        logger.debug("Starting step two...")
        context["derived_example_value"] = (
            f'Value from previous step was: {context["some_example_value"]}'
        )
        logger.debug(f"Step two done with, {context=}")

    @staticmethod
    def recover_action(generation: Generation, context: OrderedDict) -> bool:
        logger.debug(
            "Recovering (pretend) value for step two from previous pipeline run ..."
        )
        recovered_from_file = "Value for step two pretend recovered from file"
        context["derived_example_value"] = recovered_from_file
        logger.debug(f"Step two recovery done with, {context=}")
