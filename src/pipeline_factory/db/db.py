import logging

import sqlalchemy
from sqlmodel import Session, SQLModel, create_engine

from . import sqlite_config

logger = logging.getLogger(__name__)


engine = create_engine(
    sqlite_config.db_url, echo=True, connect_args=sqlite_config.connect_args
)

for event_name, event_handler in sqlite_config.event_handlers.items():
    sqlalchemy.event.listens_for(engine, event_name)(event_handler)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def drop_all_tables():
    SQLModel.metadata.drop_all(engine)


def get_session_context(expire_on_commit=True):
    return Session(engine, expire_on_commit=expire_on_commit)


def get_db():
    with get_session_context() as session:
        yield session
