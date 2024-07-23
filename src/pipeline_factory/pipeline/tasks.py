import contextlib
from enum import StrEnum
import os
from typing import Any, Optional, OrderedDict
import uuid

from .. import config
from . import controlers
from . import pipeline
from . import models


@contextlib.contextmanager
def _log_exceptions():
    try:
        yield
    except Exception as e:
        pipeline.logger.exception(e)
        pipeline.logger.debug(
            f"\n<<<<<< ERROR IN PIPELINE TASK (worker process:{os.getpid()=}) >>>>>>\n"
        )


class ErrorInPipelineStep(Exception):
    pass


DEBUG_REPEAT_CHAR_LEN = 120


def _setup_pipeline(
    pipeline_type: StrEnum,
    init_parms: Optional[dict] = None,
) -> tuple[OrderedDict, pipeline.PipelineStepSet]:
    context, pipeline_steps = pipeline.get_pipeline_step_set(pipeline_type)

    context.update({"app_config": config.get_config()})

    if init_parms:
        context.update(init_parms)

    # TODO: Pipeline init func call
    #   - add a way to register pipeline coerce func that converts initial context
    #   - add a way to register pipeline init func
    #   - call coerce and init funcs prior to executing pipeline 

    return context, pipeline_steps


def _run_pipeline(
    context: OrderedDict,
    pipeline_steps: pipeline.PipelineStepSet,
    controler_session: controlers.BaseControlerSession,
    recovery_mode: bool = False,
):
    generation = controler_session.generation

    pipeline.logger.debug("\n\n*********** PIPELINE BEGIN ***********\n")
    pipeline.logger.debug(f"Pipeline meta {generation=}, {context=}, {pipeline_steps=}")

    for step_idx, step_result in enumerate(
        pipeline.execute_step_set(
            generation, context, pipeline_steps, recovery_mode=recovery_mode
        )
    ):
        controler_session.save_step(step_result)

        pipeline.logger.debug(f"{'-' * DEBUG_REPEAT_CHAR_LEN}")
        pipeline.logger.debug(
            f"\n\nSTEP #{step_idx} FINISHED: Wrote {step_result=} to db ...\n"
        )
        pipeline.logger.debug(f"{'-' * DEBUG_REPEAT_CHAR_LEN}\n\n")

        if step_result.status == models.PipelineStepStatus.ERROR:
            raise ErrorInPipelineStep

    pipeline.logger.debug("\n*********** PIPELINE END ***********\n\n")


def first_pipeline(
    pipeline_type: str,
    params: Optional[dict[str, Any]] = None,
):
    with _log_exceptions():
        PipelineTypes = pipeline.get_pipeline_types()
        coerced_pipeline_type = PipelineTypes(pipeline_type)

        if not params:
            params = {}

        context, steps = _setup_pipeline(coerced_pipeline_type, init_parms=params)
        with controlers.get_controler().new(
            pipeline_type=coerced_pipeline_type, pipeline_context=context
        ) as controler_session:
            pipeline.logger.debug(
                f"Running pipeline of {pipeline_type=} for new {controler_session.generation=}"
            )
            _run_pipeline(context, steps, controler_session, recovery_mode=False)


def next_pipeline(
    pipeline_type: str,
    previous_pipeline_type: str,
    public_id: Optional[uuid.UUID] = None,
    selection_criteria: Optional[
        controlers.AdditionalGenerationSelectionCriteria
    ] = None,
    params: Optional[dict[str, Any]] = None,
):
    with _log_exceptions():
        PipelineTypes = pipeline.get_pipeline_types()
        coerced_pipeline_type = PipelineTypes(pipeline_type)
        coerced_previous_pipeline_type = PipelineTypes(previous_pipeline_type)

        if not params:
            params = {}

        context, steps = _setup_pipeline(coerced_pipeline_type, init_parms=params)

        if public_id:
            generation_factory = controlers.get_controler().by_public_id
            factory_params = {
                "public_id": public_id,
                "pipeline_context": context,
                "pipeline_type": coerced_previous_pipeline_type,
            }
        elif selection_criteria:
            generation_factory = controlers.get_controler().ready_for_pipeline
            factory_params = {
                "selection_criteria": selection_criteria,
                "pipeline_type": coerced_previous_pipeline_type,
                "pipeline_context": context,
            }
        else:
            raise RuntimeError(
                "Must provide public_id or selection criteria to select Generation"
            )

        with generation_factory(**factory_params) as controler_session:
            pipeline.logger.debug(
                f"Running pipeline of {pipeline_type=} for {controler_session.generation=}"
            )
            _run_pipeline(context, steps, controler_session, recovery_mode=False)


def recover_pipeline(
    pipeline_type: str,
    public_id: Optional[uuid.UUID] = None,
    selection_criteria: Optional[
        controlers.AdditionalGenerationSelectionCriteria
    ] = None,
    params: Optional[dict[str, Any]] = None,
):
    with _log_exceptions():
        PipelineTypes = pipeline.get_pipeline_types()
        coerced_pipeline_type = PipelineTypes(pipeline_type)

        if not params:
            params = {}

        context, steps = _setup_pipeline(coerced_pipeline_type, init_parms=params)

        if public_id:
            generation_factory = controlers.get_controler().by_public_id
            factory_params = {
                "public_id": public_id,
                "pipeline_type": coerced_pipeline_type,
                "pipeline_context": context,
            }
        elif selection_criteria:
            generation_factory = controlers.get_controler().ready_for_pipeline
            factory_params = {
                "selection_criteria": selection_criteria,
                "pipeline_type": coerced_pipeline_type,
                "pipeline_context": context,
            }
        else:
            raise RuntimeError(
                "Must provide public_id or selection criteria to select Generation"
            )

        with generation_factory(**factory_params) as controler_session:
            pipeline.logger.debug(
                f"Recovering pipeline of {pipeline_type=} for {controler_session.generation=}"
            )
            _run_pipeline(context, steps, controler_session, recovery_mode=True)
