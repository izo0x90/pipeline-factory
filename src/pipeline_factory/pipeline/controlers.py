from contextlib import contextmanager
from enum import StrEnum
import logging
from typing import Generator, Mapping, Optional, Protocol
import uuid

import pydantic

from . import models


logger = logging.getLogger(__name__)


GenerationPipelinesInfo = list[models.Generation]
GenerationInfo = list[models.GenerationNoPipelines]


class AdditionalGenerationSelectionCriteria(pydantic.BaseModel):
    pipeline_status: Optional[str] = None
    errored: bool = False


class BaseControlerSession(Protocol):
    @property
    def generation(self) -> models.Generation: ...

    def save_step(self, step: models.PipelineStep) -> None: ...


class BaseControler(Protocol):
    def get_all_generation_pipelines(
        self,
        *,
        generation_ids: Optional[list[uuid.UUID]],
        states: Optional[list[models.GenerationStateType]],
        n_newset: Optional[int],
        with_pipelines: bool = True,
    ) -> GenerationPipelinesInfo | GenerationInfo: ...

    @contextmanager
    def new(
        self, pipeline_type: StrEnum, pipeline_context: Mapping
    ) -> Generator[BaseControlerSession, None, None]: ...

    @contextmanager
    def by_public_id(
        self, public_id: uuid.UUID, pipeline_type: StrEnum, pipeline_context: Mapping
    ) -> Generator[BaseControlerSession, None, None]: ...

    @contextmanager
    def ready_for_pipeline(
        self,
        pipeline_type: StrEnum,
        pipeline_context: Mapping,
        selection_criteria: Optional[AdditionalGenerationSelectionCriteria] = None,
    ) -> Generator[BaseControlerSession, None, None]: ...


_controler: Optional[BaseControler] = None


def register_controler(controler: BaseControler):
    global _controler
    _controler = controler


def get_controler() -> BaseControler:
    global _controler

    if not _controler:
        raise RuntimeError("Controller not configured")

    return _controler
