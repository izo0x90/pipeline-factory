# SQLite uses db scale locks and there are known issues with pysqlite driver
# those get addressed here and we also use helper methods to take a db op and release
# the db to allow for some concurrency for our worker processes
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import StrEnum
import time
from typing import Callable, Generator, Mapping, Optional, Sequence
import uuid

import sqlalchemy
from sqlmodel.sql.base import Executable
from sqlmodel import Column, Field, select, SQLModel, Relationship, JSON

from .db import get_session_context, logger
from ..pipeline import controlers, models, pipeline


class FailedToGetSQLiteDBLock(Exception):
    """When retry fails to obtain lock for SQLite DB after max attempts"""


def retry_db(sleep_in_seconds=10, max_retries=10):
    def decorator(func):
        def wrapper(*args, **kwargs):
            count = 0
            while count < max_retries:
                try:
                    return func(*args, **kwargs)
                except sqlalchemy.exc.OperationalError as e:
                    if (
                        e.args
                        and hasattr((err := e.args[0]), "endswith")
                        and err.endswith("database is locked")
                    ):
                        logger.exception(e)
                        logger.debug(f"Attempting dbop {func} failed, waiting ...")
                        count += 1
                        time.sleep(sleep_in_seconds)
                    else:
                        raise

            raise FailedToGetSQLiteDBLock(
                f"After {max_retries=} and {(max_retries * sleep_in_seconds)=} wait time retry failed to get db access operation {func} aborted"
            )

        return wrapper

    return decorator


@retry_db(sleep_in_seconds=20, max_retries=100)
def sync_write_to_db_wait_on_session(
    model_instances: list[SQLModel],
) -> Sequence[SQLModel]:
    """Using this be aware that you can be working with expired data"""
    with get_session_context(expire_on_commit=False) as session:
        # expire_on_commit allow to use model instances outside of session
        session.add_all(model_instances)
        session.commit()

        return model_instances


@retry_db(sleep_in_seconds=20, max_retries=100)
def sync_exec_and_commit_wait_on_session(func: Callable) -> Optional[Sequence]:
    with get_session_context(expire_on_commit=False) as session:
        results = func(session)
        session.commit()
        return results


@retry_db(sleep_in_seconds=20, max_retries=100)
def sync_read_from_db_wait_on_session(statement: Executable, unique=False) -> Sequence:
    with get_session_context() as session:
        exec = session.exec(statement)

        if unique:
            exec = exec.unique()

        return exec.all()


class Generation(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    public_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    pipelines: Optional[list["Pipeline"]] = Relationship(back_populates="generation")
    state: models.GenerationStateType | None = None


class Pipeline(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    generation_id: int = Field(default=None, foreign_key="generation.id")
    generation: Generation = Relationship(back_populates="pipelines")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    steps: Optional[list["PipelineStep"]] = Relationship(back_populates="pipeline")
    pipeline_type: str
    status: Optional[str] = None
    errored: bool = False
    initial_context: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default="{}"),
    )


class PipelineStep(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    pipeline_id: int = Field(default=None, foreign_key="pipeline.id")
    pipeline: Pipeline = Relationship(back_populates="steps")
    name: str
    status: models.PipelineStepStatus
    took: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ControlerSession:
    def __init__(self, generation: Generation) -> None:
        self._generation = generation

    @property
    def generation(self) -> models.Generation:
        return models.Generation(**self._generation.model_dump())

    def save_step(self, step: models.PipelineStep) -> None:
        if not self._generation.pipelines:
            RuntimeError(
                "Attempting to save step status outside of pipeline execution pipeline_context"
            )

        current_pipeline = self._generation.pipelines[-1]

        pipeline_step = PipelineStep(
            **step.model_dump(mode="python"), pipeline=current_pipeline
        )

        sync_write_to_db_wait_on_session([pipeline_step])
        return

    def set_states(
        self,
        generation_target_state: models.GenerationStateType,
        pipeline_target_status: StrEnum | None,
        errored: bool = False,
    ) -> None:
        def set_status(session):
            # Atomic only for sqlite because of global session lock
            session.add(self._generation)

            if not self._generation.pipelines:
                RuntimeError(
                    "Attempting to set pipeline status outside of pipeline execution pipeline_context"
                )

            already_has_state = self._generation.state == generation_target_state

            if already_has_state:
                logger.warn(
                    f"Attempting to set same state for {self._generation.state}, "
                    "{current_state=} == {target_state=}. "
                    "Not setting 'new' redundant state!"
                )

            self._generation.state = generation_target_state

            current_pipeline = self._generation.pipelines[-1]
            already_has_state = current_pipeline.status == pipeline_target_status

            if already_has_state:
                logger.warn(
                    f"Attempting to set same state for {current_pipeline.status}, "
                    "{current_state=} == {target_state=}. "
                    "Not setting 'new' redundant state!"
                )

            current_pipeline.status = pipeline_target_status

            if errored:
                current_pipeline.errored = True

        sync_exec_and_commit_wait_on_session(set_status)


class SqliteControler:
    def get_all_generation_pipelines(
        self,
        *,
        generation_ids: Optional[list[uuid.UUID]],
        states: Optional[list[models.GenerationStateType]],
        n_newset: Optional[int],
        with_pipelines: bool = True,
    ) -> controlers.GenerationPipelinesInfo | controlers.GenerationInfo:
        statement = select(Generation)

        if with_pipelines:
            statement = statement.options(
                sqlalchemy.orm.joinedload(Generation.pipelines).joinedload(
                    Pipeline.steps
                )
            )

        if generation_ids:
            statement = statement.where(Generation.public_id.in_(generation_ids))

        if states:
            statement = statement.where(Generation.state.in_(states))

        if n_newset:
            statement = statement.order_by(Generation.id.desc()).limit(n_newset)

        results = sync_read_from_db_wait_on_session(statement, unique=True)

        if with_pipelines:
            pipelines_info = controlers.GenerationPipelinesInfo(results)
        else:
            pipelines_info = controlers.GenerationInfo(results)

        return pipelines_info

    @contextmanager
    def _setup_generation(
        self, generation: Generation, pipeline_type: StrEnum, pipeline_context: Mapping
    ) -> Generator[controlers.BaseControlerSession, None, None]:
        current_pipeline = Pipeline(generation=generation, pipeline_type=pipeline_type)
        sync_write_to_db_wait_on_session([generation, current_pipeline])

        controler_session = ControlerSession(generation=generation)
        starting_pipeline_status = pipeline_context.get(pipeline.STATUS_KEY, None)

        controler_session.set_states(
            models.GenerationStateType.GENERATION_IN_PROGRESS, starting_pipeline_status
        )

        try:
            yield controler_session

        except Exception as e:
            logger.exception(e)
            logger.debug(f"Errored while in open session for {generation=}")
            controler_session.set_states(
                models.GenerationStateType.LAST_ERRORED, None, True
            )
        else:
            final_pipeline_status = pipeline_context.get(pipeline.STATUS_KEY, None)
            controler_session.set_states(
                models.GenerationStateType.GENERATION_DONE, final_pipeline_status
            )
            logger.debug(
                f"Pipeline for {generation=} finished with {final_pipeline_status=}"
            )

    @contextmanager
    def new(
        self, pipeline_type: StrEnum, pipeline_context: Mapping
    ) -> Generator[controlers.BaseControlerSession, None, None]:
        generation = Generation()
        with self._setup_generation(
            generation, pipeline_type, pipeline_context
        ) as session:
            yield session

    @contextmanager
    def by_public_id(
        self,
        public_id: uuid.UUID,
        pipeline_type: StrEnum,
        pipeline_context: Mapping,
        force: bool = False,
    ) -> Generator[controlers.BaseControlerSession, None, None]:
        def select_generation(session):
            statement = select(Generation).where(Generation.public_id == public_id)

            if not force:
                statement = statement.where(
                    Generation.state == models.GenerationStateType.GENERATION_DONE
                )

            results = session.exec(statement).all()

            if not results:
                return

            generation, *_ = results

            # Set gen. state to None, "faux lock" from being selected by other pipelines
            # Only works on sqlite because global lock while in session
            generation.state = None

            return results

        results = sync_exec_and_commit_wait_on_session(select_generation)
        if not results:
            raise RuntimeError(f"Generation with {public_id=} not found")

        generation, *_ = results

        with self._setup_generation(
            generation, pipeline_type, pipeline_context
        ) as session:
            yield session

    @contextmanager
    def ready_for_pipeline(
        self,
        pipeline_type: StrEnum,
        pipeline_context: Mapping,
        selection_criteria: Optional[
            controlers.AdditionalGenerationSelectionCriteria
        ] = None,
    ) -> Generator[controlers.BaseControlerSession, None, None]:
        def select_generation(session):
            statement = (
                select(Generation)
                .outerjoin(Pipeline, Pipeline.generation_id == Generation.id)
                .where(
                    Generation.state == models.GenerationStateType.GENERATION_DONE,
                    Pipeline.pipeline_type == pipeline_type,
                )
            )

            if selection_criteria:
                statement = statement.where(
                    Pipeline.status == selection_criteria.pipeline_status,
                    Pipeline.errored == selection_criteria.errored,
                )

            results = session.exec(statement).all()

            if not results:
                return

            generation, *_ = results

            # Set gen. state to None, "faux lock" from being selected by other pipelines
            # Only works on sqlite because global lock while in session
            generation.state = None

            return results

        results = sync_exec_and_commit_wait_on_session(select_generation)
        if not results:
            raise RuntimeError("Matching Generation not found")

        generation, *_ = results

        with self._setup_generation(
            generation, pipeline_type, pipeline_context
        ) as session:
            yield session
