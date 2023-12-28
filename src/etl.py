import backoff
import elastic_transport
import psycopg2

from collections.abc import Generator
from functools import wraps
from typing import Tuple, Any

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow

from core import config
from core.config import DB_CONNECT, logger

from schemas import ModelNames
from state import State
from utils import tranfsorm as transform_utils
from utils import extract as extract_utils
from utils.backoff import _backoff, _giveup


def coroutine(func):
    @wraps(func)
    def inner(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Generator:
        fn: Generator = func(*args, **kwargs)
        next(fn)
        return fn

    return inner

@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      on_giveup=_giveup,
                      on_backoff=_backoff,
                      max_tries=5,
                      jitter=backoff.random_jitter,
                      max_time=16)
def extract(batch: Generator) -> None:
    with psycopg2.connect(**DB_CONNECT) as conn:
        # run pipe by film's changes
        fw_state = State(ModelNames.FILMWORK)
        fw_extractor = extract_utils.FilmworkExtractor(conn, fw_state)
        fw_extractor.pipe(batch)

        # run by genre's changes
        g_state = State(ModelNames.GENRE)
        g_extractor = extract_utils.FilmworkExtractorByGenre(conn, g_state)
        g_extractor.pipe(batch)

        # run by genre's changes
        p_state = State(ModelNames.PERSON)
        p_extractor = extract_utils.FilmworkExtractorByPerson(conn, p_state)
        p_extractor.pipe(batch)


@coroutine
def transform(batch: Generator) -> Generator[None, DictRow, None]:
    while records := (yield):
        bulk = transform_utils.agregate(records)

        batch.send(bulk)


@backoff.on_exception(backoff.expo,
                      elastic_transport.ConnectionError,
                      on_giveup=_giveup,
                      on_backoff=_backoff,
                      max_tries=5,
                      jitter=backoff.random_jitter,
                      max_time=16)
@coroutine
def load() -> Generator[None, Tuple, None]:

    with Elasticsearch(config.ELASTIC_CONNECT) as client:
        if not client.ping():
            raise elastic_transport.ConnectionError("Failed check connections")
        while subjects := (yield):
            resp = helpers.bulk(client=client, actions=subjects)
            logger.info(f"{resp[0]} documents has been updated.")
