from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional
import uuid

import pydantic


class PipelineStepStatus(StrEnum):
    DONE = "DONE"
    ERROR = "ERROR"


class GenerationStateType(StrEnum):
    LAST_ERRORED = "LAST_ERRORED"
    GENERATION_IN_PROGRESS = "IN_PROGRESS"
    GENERATION_DONE = "DONE"


class PipelineStep(pydantic.BaseModel):
    id: int = pydantic.Field(default=None)
    name: str
    status: PipelineStepStatus
    took: float
    created_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class Pipeline(pydantic.BaseModel):
    id: int
    created_at: datetime
    steps: list[PipelineStep] | None
    pipeline_type: str
    status: Optional[str]
    errored: bool = False


class Generation(pydantic.BaseModel):
    id: int
    public_id: uuid.UUID
    created_at: datetime
    pipelines: list[Pipeline] | None = None
    state: GenerationStateType | None


class GenerationNoPipelines(pydantic.BaseModel):
    id: int
    public_id: uuid.UUID
    created_at: datetime
    state: GenerationStateType | None
