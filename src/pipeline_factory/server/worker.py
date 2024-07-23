import asyncio
from asyncio.tasks import Task
from datetime import datetime
import json
import logging
import inspect
import multiprocessing
from multiprocessing import pool
import pathlib
import signal
import os
from types import ModuleType
from typing import Any, Callable, ClassVar, Mapping, Optional, Self, IO

from croniter import croniter
import pydantic


logger = logging.getLogger(__name__)


def initializer(load_pipelines):
    """
    - Make sure logging is configured on worker processes.
    - Load pipeline script on workers only.
    - Ignore CTRL+C in the worker process.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logger.debug("Loading pipelines ...")
    load_pipelines()
    pid = os.getpid()
    logger.debug(f"Worker process initialized {pid=} ...")


class ScheduledTaskConfig(pydantic.BaseModel):
    name: str
    cron_str: str
    params: dict[str, Any] = pydantic.Field(default_factory=dict)


class ScheduledTaskMeta(ScheduledTaskConfig):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    schedule_task: Task = pydantic.Field(exclude=True)


class ScheduledTaskLedger(pydantic.BaseModel):
    schedule: dict[str, ScheduledTaskMeta]


class ScheduledConfig(pydantic.BaseModel):
    schedule: dict[str, ScheduledTaskConfig]

    @classmethod
    def from_file(cls, file_or_path: pathlib.Path | IO) -> Optional[Self]:
        try:
            if isinstance(file_or_path, pathlib.Path):
                with open(file_or_path, "r") as f:
                    data = f.read()
            else:
                data = file_or_path.read()

            return cls.model_validate(json.loads(data))
        except (FileNotFoundError, json.JSONDecodeError, pydantic.ValidationError) as e:
            logger.error("Could not load task schedule!")
            logger.exception(e)

        return


class WorkerManager:
    instance: ClassVar[Optional[Self]] = None
    _initialized = False

    def __init__(self, load_pipelines: Optional[Callable] = None):
        if self._initialized:
            return

        if not load_pipelines:
            raise RuntimeError(
                "Callable that imports pipeline definitions is required to register them on workers"
            )

        self.pool = multiprocessing.Pool(
            processes=3, initializer=initializer, initargs=(load_pipelines,)
        )
        self.active_tasks = ScheduledTaskLedger(schedule={})
        self._initialized = True

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = super().__new__(cls)

        return cls.instance

    def get_callable_by_name(
        self, module: ModuleType, callable_name: str
    ) -> Optional[Callable]:
        return getattr(module, callable_name, None)

    def get_all_callable(self, module: ModuleType):
        callable_names = inspect.getmembers(module, inspect.isfunction)
        callables_meta = {}
        for name, _ in callable_names:
            if name.startswith("_"):
                continue

            callable = getattr(module, name)
            signature = inspect.signature(callable)

            params_meta = {}
            for param in signature.parameters.values():
                if issubclass(type(param.annotation), type):
                    type_str = param.annotation.__name__
                else:
                    type_str = str(param.annotation)

                params_meta[param.name] = type_str
            callables_meta[name] = params_meta

        return callables_meta

    def validate_task_kwargs(
        self, task: Callable, kwargs: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        params = {}
        signature = inspect.signature(task)
        for param in signature.parameters.values():
            if param.name in kwargs:
                try:
                    value = pydantic.TypeAdapter(param.annotation).validate_python(
                        kwargs[param.name]
                    )

                except pydantic.ValidationError:
                    raise ValueError(f"Expected {param.annotation} for {param.name=}")

                params[param.name] = value
            elif param.default is inspect.Parameter.empty:
                raise ValueError(
                    f"No value or default for {param.name=} of type {param.annotation}"
                )

        return params

    def run(self, task: Callable, **kwargs) -> pool.AsyncResult:
        if not self.pool:
            raise LookupError("Missing process pool")

        coerced_kwargs = self.validate_task_kwargs(task, kwargs)
        return self.pool.apply_async(task, kwds=coerced_kwargs)

    def deschedule(self, schedule_task_name: str):
        if schedule_task_name not in self.active_tasks.schedule:
            raise ValueError("No task with {schedule_task_name} name found")

        self.active_tasks.schedule[schedule_task_name].schedule_task.cancel()
        self.active_tasks.schedule.pop(schedule_task_name)

    def deschedule_all(self):
        for name in self.active_tasks.schedule.keys():
            self.deschedule(name)

    def get_scheduled(self) -> ScheduledTaskLedger:
        return self.active_tasks

    def schedule(self, cron: str, task: Callable, name: Optional[str] = None, **kwargs):
        if not croniter.is_valid(cron):
            raise ValueError("Invalid cron expression")

        if name:
            try:
                self.deschedule(name)
            except ValueError:
                pass

        schedule_task = asyncio.ensure_future(
            delayed(cron, task, worker_manager=self, **kwargs)
        )
        schedule_task.set_name(name)
        self.active_tasks.schedule[schedule_task.get_name()] = ScheduledTaskMeta(
            name=task.__name__,
            schedule_task=schedule_task,
            cron_str=cron,
            params=kwargs,
        )

        return schedule_task.get_name()

    def bulk_schedule(self, schedulable_tasks: ModuleType, config: ScheduledConfig):
        for scheduled_task_name, task_config in config.schedule.items():
            callable = self.get_callable_by_name(schedulable_tasks, task_config.name)
            if not callable:
                logger.error("Task {task_config.name=} not found, not scheduling")
                continue

            self.schedule(
                task_config.cron_str,
                callable,
                scheduled_task_name,
                **task_config.params,
            )

    def __del__(self):
        if self.pool:
            self.pool.terminate()
            self.pool.close()

    def shutdown(self):
        self.__del__()


def get_worker_manager():
    return WorkerManager(None)


async def delayed(cron, task, worker_manager, **kwargs):
    count = 0
    while True:
        count += 1
        in_seconds = get_delta(cron)
        await asyncio.sleep(in_seconds)
        worker_manager.run(task, **kwargs)


def get_delta(cron):
    """
    This function returns the time delta between now and the next cron execution time.
    Credit: https://github.com/priyanshu-panwar
    """
    now = datetime.now()
    cron = croniter(cron, now)
    return (cron.get_next(datetime) - now).total_seconds()
