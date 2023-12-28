import psycopg2

from collections.abc import Generator
from functools import wraps
from typing import Tuple, Any

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow

from core import config
from core.config import DB_CONNECT

from schemas import ModelNames
from state import State
from utils import tranfsorm as transform_utils
from utils.extract import (
    FilmworkExtractor,
    FilmworkExtractorByGenre,
    FilmworkExtractorByPerson,
)


def coroutine(func):
    @wraps(func)
    def inner(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Generator:
        fn: Generator = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def extract(batch: Generator) -> None:
    with psycopg2.connect(**DB_CONNECT) as conn:
        # run pipe by film's changes
        fw_state = State(ModelNames.FILMWORK)
        fw_extractor = FilmworkExtractor(conn, fw_state)
        fw_extractor.pipe(batch)

        # run by genre's changes
        g_state = State(ModelNames.GENRE)
        g_extractor = FilmworkExtractorByGenre(conn, g_state)
        g_extractor.pipe(batch)

        # run by genre's changes
        p_state = State(ModelNames.PERSON)
        p_extractor = FilmworkExtractorByPerson(conn, p_state)
        p_extractor.pipe(batch)


@coroutine
def transform(batch: Generator) -> Generator[None, DictRow, None]:
    while records := (yield):
        bulk = transform_utils.agregate(records)

        batch.send(bulk)


@coroutine
def load() -> Generator[None, Tuple, None]:
    while subjects := (yield):
        with Elasticsearch(config.ELASTIC_CONNECT) as client:
            resp = helpers.bulk(client=client, actions=subjects)
            config.logger.info(f"{resp[0]} documents has been updated.")
