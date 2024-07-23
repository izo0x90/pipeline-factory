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
from pipeline_factory.utils import dir_path_for_generation

from action_clients.text_generate_gemini import (
    GENERIC_PROMPT,
    RESPONSE_DELIMITER,
    PromptBuilder,
    QueryVertexModel,
)

logger = logging.getLogger(__name__)


class PipelineStatus(StrEnum):
    GENERATED_SPEECH = "Generated Speech"
    RECOVERED_SPEECH = "Recovered Speech"


@register_step(PipelineTypes.SPEECH_GEN)
def create_prompt(generation: Generation, context: OrderedDict) -> None:
    logger.debug("Generating prompt ...")
    prompt_builder = PromptBuilder()
    context["prompt"] = prompt_builder(
        prompt_template=GENERIC_PROMPT,
        person_full_name="Alan Watts",
        subject="Ice Skating",
    )
    logger.debug(f"Step one done with, {context=}")


@register_step(PipelineTypes.SPEECH_GEN)
class GenerateSpeech(RecoverableStepType):
    @staticmethod
    def step_action(generation: Generation, context: OrderedDict) -> None:
        logger.debug("Generating speech ...")
        app_config = context["app_config"]
        llm_client = QueryVertexModel(
            key=app_config.GOOGLE_API_KEY,
            prompt=context["prompt"],
            result_delimiter=RESPONSE_DELIMITER,
        )
        speech = llm_client.get_text()
        context["speech"] = speech
        output_path = dir_path_for_generation(generation.public_id, "speech.txt")
        os.makedirs(output_path.parent)
        with open(output_path, "w") as f:
            f.write(speech)
        context[STATUS_KEY] = PipelineStatus.GENERATED_SPEECH
        logger.debug(f"Step two done with, {context=}")

    @staticmethod
    def recover_action(generation: Generation, context: OrderedDict) -> bool:
        logger.debug("Recovering speech ...")
        output_path = dir_path_for_generation(generation.public_id, "speech.txt")
        with open(output_path, "r") as f:
            speech = f.read()
        context["speech"] = speech
        context[STATUS_KEY] = PipelineStatus.RECOVERED_SPEECH
        logger.debug(f"Step two recovery done with, {context=}")
        return True
