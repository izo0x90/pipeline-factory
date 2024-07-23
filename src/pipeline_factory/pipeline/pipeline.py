from abc import ABC
from enum import StrEnum
from functools import wraps
import logging
from typing import Callable, Generator, OrderedDict, Protocol, Union, cast
import time

from .models import Generation, PipelineStep, PipelineStepStatus

logger = logging.getLogger(__name__)

STATUS_KEY = "pipeline_status"


class RecoverableStepType(ABC):
    is_recoverable = True

    @classmethod
    def run(
        cls, generation: Generation, context: OrderedDict, recovery_mode: bool
    ) -> bool:
        if recovery_mode:
            if success := cls.recover_action(generation, context):
                return success

        cls.step_action(generation, context)
        return False

    @staticmethod
    def step_action(generation: Generation, context: OrderedDict) -> None: ...

    @staticmethod
    def recover_action(generation: Generation, context: OrderedDict) -> bool: ...


StepType = Union[Callable, RecoverableStepType]


class WrappedStepCallable(Protocol):
    def __call__(
        self, generation: Generation, context: OrderedDict, recovery_mode: bool
    ) -> tuple[float, bool]: ...


RegisteredPipelines = OrderedDict[StrEnum, OrderedDict[str, WrappedStepCallable]]

_registered_pipelines: RegisteredPipelines = RegisteredPipelines()

_pipeline_types: StrEnum


def register_pipeline_types(types: StrEnum):
    global _pipeline_types

    _pipeline_types = types


def get_pipeline_types() -> StrEnum:
    global _pipeline_types
    return _pipeline_types


def register_step(pipeline_name: StrEnum):
    global _registered_pipelines

    def decorator(callable: StepType):
        pipeline = _registered_pipelines.setdefault(pipeline_name, OrderedDict())

        @wraps(callable)
        def wrapper(
            generation: Generation,
            context: OrderedDict,
            *,
            recovery_mode: bool = False,
        ) -> tuple[float, bool]:
            t_start = time.perf_counter()

            try:
                is_recoverable = cast(RecoverableStepType, callable).is_recoverable
            except AttributeError:
                is_recoverable = False

            if is_recoverable:
                recovered = cast(RecoverableStepType, callable).run(
                    generation, context, recovery_mode=recovery_mode
                )
            else:
                recovered = recovery_mode
                cast(Callable, callable)(generation, context)
            duration = time.perf_counter() - t_start
            return (duration, recovered)

        pipeline[callable.__name__] = cast(WrappedStepCallable, wrapper)
        logger.debug(f"Register step {callable.__name__} for {pipeline_name.value}")

        return wrapper

    return decorator


PipelineExecGenerator = Generator[PipelineStep, None, None]
PipelineStepSet = tuple[StrEnum, OrderedDict[str, WrappedStepCallable]]


def _get_pipeline_steps(pipeline_type):
    return _registered_pipelines[pipeline_type]


def get_pipeline_step_set(
    pipeline_type: StrEnum,
) -> tuple[OrderedDict, PipelineStepSet]:
    context = OrderedDict()
    pipeline_steps = _get_pipeline_steps(pipeline_type)
    return context, (pipeline_type, pipeline_steps)


def execute_step_set(
    generation: Generation,
    context: OrderedDict,
    pipeline_steps_set: PipelineStepSet,
    recovery_mode: bool = False,
) -> PipelineExecGenerator:
    previous_step_errored = False
    _, pipeline_steps = pipeline_steps_set
    still_recovering = recovery_mode
    for step_name, step in pipeline_steps.items():
        if previous_step_errored:
            break

        try:
            (took, recovery_success) = step(
                generation, context, recovery_mode=still_recovering
            )
            still_recovering = recovery_success and recovery_mode

            step_result = PipelineStep(
                name=step_name,
                status=PipelineStepStatus.DONE,
                took=took,
            )
        except Exception as e:
            logger.exception(e)
            step_result = PipelineStep(
                name=step_name,
                status=PipelineStepStatus.ERROR,
                took=0.0,
            )
            previous_step_errored = True

        yield step_result
