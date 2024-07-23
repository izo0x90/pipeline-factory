from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from importlib.resources import files
import os
import logging
from typing import Annotated, Callable, Optional, Sequence
import uuid

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    Query,
    UploadFile,
)
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import DictLoader, Environment

from .. import config
from .. import pipeline
from . import tasks
from . import worker

logger = logging.getLogger(__name__)


def create_app(
    app_config: config.AppConfig,
    pipeline_types: StrEnum,
    controler: pipeline.controlers.BaseControler,
    load_pipelines: Callable,
    additional_tasks: Sequence[Callable],
):
    config.register_config(app_config)
    pipeline.pipeline.register_pipeline_types(pipeline_types)
    pipeline.controlers.register_controler(controler)

    for task in additional_tasks:
        setattr(tasks, task.__name__, task)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # Application startup
        wm = worker.WorkerManager(load_pipelines=load_pipelines)
        task_schedule = worker.ScheduledConfig.from_file(app_config.TASK_SCHEDULE_FILE)
        if task_schedule:
            wm.bulk_schedule(tasks, task_schedule)
        yield
        # Application shutdown
        worker.get_worker_manager().shutdown()

    factory_server = FastAPI(lifespan=lifespan, dependencies=[])

    # Task routes
    @factory_server.get("/task/")
    async def get_available_tasks(worker_manager=Depends(worker.get_worker_manager)):
        try:
            task_schedule = worker_manager.get_all_callable(tasks)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return task_schedule

    @factory_server.post("/task/")
    def run_task(
        task_name: str,
        task_params: Optional[dict] = None,
        worker_manager=Depends(worker.get_worker_manager),
    ):
        try:
            task = worker_manager.get_callable_by_name(tasks, task_name)
            task_params = task_params or {}
            worker_manager.run(task, **task_params)
        except Exception as e:
            # raise e
            raise HTTPException(status_code=500, detail=str(e))

        return {"response": "success"}

    # Generation routes
    GenerationIdsParam = Optional[list[uuid.UUID]]
    GenerationStatesParam = Optional[list[pipeline.models.GenerationStateType]]

    async def get_generation_state_and_history(
        generation_ids: Annotated[GenerationIdsParam, Query()] = None,
        states: Annotated[GenerationStatesParam, Query()] = None,
        n_newset: Optional[int] = None,
        with_pipelines: bool = False,
    ):
        try:
            pipelines = (
                pipeline.controlers.get_controler().get_all_generation_pipelines(
                    generation_ids=generation_ids,
                    states=states,
                    n_newset=n_newset,
                    with_pipelines=with_pipelines,
                )
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return pipelines

    get_generation_state_and_history.__doc__ = f"Generation states: {tuple(v.value for v in pipeline.models.GenerationStateType)}"
    factory_server.get(
        "/generation/",
        response_model=pipeline.controlers.GenerationPipelinesInfo
        | pipeline.controlers.GenerationInfo,
    )(get_generation_state_and_history)

    # Schedule routes
    @factory_server.get("/schedule-task/")
    async def get_schedule_task(
        schedule_only: bool = False, worker_manager=Depends(worker.get_worker_manager)
    ):
        try:
            task_schedule = worker_manager.get_scheduled()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        if schedule_only:
            return task_schedule

        return {"now": datetime.now(), "task_schedule": task_schedule}

    @factory_server.post("/schedule-task/")
    async def schedule_task(
        task_name: Optional[str] = None,
        task_params: Optional[dict] = None,
        cron: Optional[str] = None,
        cancel_task_id: Optional[str] = None,
        worker_manager=Depends(worker.get_worker_manager),
    ):
        """
        <a href="https://crontab.cronhub.io/" target="_blank">https://crontab.cronhub.io/</a>
        """
        task_id = None
        try:
            if cancel_task_id:
                worker_manager.deschedule(cancel_task_id)
                action = "De-scheduled"
            elif cron:
                task = worker_manager.get_callable_by_name(tasks, task_name)
                if not task:
                    raise ValueError(f"No task with name {task_name} found")

                task_params = task_params or {}
                task_id = worker_manager.schedule(cron, task, **task_params)
                action = "Scheduled"
            else:
                raise ValueError("No parameters given")
        except Exception as e:
            # raise e
            raise HTTPException(status_code=500, detail=str(e))

        return {"taskId": task_id or cancel_task_id, "action": action}

    @factory_server.post("/schedule-task-bulk/")
    async def create_upload_file(
        file: Optional[UploadFile] = None,
        remove_current: bool = False,
        worker_manager=Depends(worker.get_worker_manager),
    ):
        ok = {"response": "success"}

        try:
            if remove_current:
                worker_manager.deschedule_all()

            if file:
                task_schedule = worker.ScheduledConfig.from_file(file.file)

                if not task_schedule:
                    raise ValueError("Not able to parse schedule file")

                with open(app_config.TASK_SCHEDULE_FILE, "wb") as out_file:
                    await file.seek(0)
                    data = await file.read()
                    out_file.write(data)

                ok["filename"] = file.filename or ""
            else:
                task_schedule = worker.ScheduledConfig.from_file(
                    app_config.TASK_SCHEDULE_FILE
                )
            worker_manager.bulk_schedule(tasks, task_schedule)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return ok

    # Browse produced artifacts
    template_dir = files("pipeline_factory.server.templates").iterdir()
    templates_dict = {}
    for file in template_dir:
        body = file.read_text()
        name = file.name
        templates_dict.update({name: body})

    output_dir = StaticFiles(directory=app_config.OUTPUT_DIR)
    factory_server.mount("/output_files", output_dir, name="output")
    loader = DictLoader(templates_dict)
    env = Environment(loader=loader)
    templates = Jinja2Templates(env=env)

    @factory_server.get("/output/{subdir:path}", response_class=HTMLResponse)
    def list_files(request: Request, subdir: str):
        path = app_config.OUTPUT_DIR
        subdir_url_path = ""
        if subdir:
            path /= subdir
            subdir_url_path = f"{subdir}/"

        files = os.listdir(path)
        file_paths = []

        for f in files:
            full_file_path = path / f
            prefix = "/output"
            if not os.path.isdir(full_file_path):
                prefix += "_files"

            file_paths.append(f"{prefix}/{subdir_url_path}{f}")

        file_paths = sorted(file_paths)

        logger.debug(f"Browsing {file_paths=}")
        return templates.TemplateResponse(
            "list_files.html",
            {"request": request, "files": file_paths},
        )

    # Test routes
    @factory_server.get("/ping/")
    def test_route():
        return {"resonse": "pong"}

    @factory_server.post("/test-task/")
    def test_task(worker_manager=Depends(worker.get_worker_manager)):
        worker_manager.run(tasks.test_task)

    return factory_server
