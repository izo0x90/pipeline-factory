import logging

from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from pipeline_factory.config import AppConfig
from pipeline_factory.server.main import create_app
from pipeline_factory.db.db import create_db_and_tables
from pipeline_factory.db.sqlite import SqliteControler
from pipeline_factory import utils
import uvicorn

from pipelines.types import PipelineTypes

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

pipeline_logger = logging.getLogger("pipeline_factory")
pipeline_logger.setLevel(logging.DEBUG)


def load_pipelines():
    from pipelines import example_pipeline_1, example_pipeline_2


def additional_test_task():
    logger.info("TESTING")


load_dotenv()


class Config(AppConfig):
    GOOGLE_API_KEY: str = utils.get_env_or_fail("VERTEX_API_KEY")


app_config = Config()

factory_server = create_app(
    app_config=app_config,
    pipeline_types=PipelineTypes,
    controler=SqliteControler(),
    load_pipelines=load_pipelines,
    additional_tasks=(additional_test_task,),
)

factory_server.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def start():
    uvicorn.run(
        "main:factory_server",
        host="0.0.0.0",
        port=5005,
        reload=True,
    )


if __name__ == "__main__":
    create_db_and_tables()
