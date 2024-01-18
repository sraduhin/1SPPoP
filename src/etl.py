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

from config import Models, ETLConfig, ExtractorConfig
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


class Etl:

    def __init__(self, model: str):
        self.config = ETLConfig(model=model)
        self.extractor = extract_utils.CombinedExtractor(model=model)

    @backoff.on_exception(backoff.expo,
                          psycopg2.OperationalError,
                          on_giveup=_giveup,
                          on_backoff=_backoff,
                          max_tries=5,
                          jitter=backoff.random_jitter,
                          max_time=16)
    def extract(self, batch: Generator) -> None:
        # run pipe by film's changes
        # self.extractor.pipe(batch, self.state)
        with psycopg2.connect(**DB_CONNECT) as connect:
            self.extractor.pipe(batch, connect)

            # g_extractor = extract_utils.FilmworkExtractorByGenre(self.config.name)
            # g_extractor.pipe(self.state, batch, connect)
            # # run by genre's changes
            # g_state = State(ModelNames.GENRE)
            # g_extractor = extract_utils.FilmworkExtractorByGenre(conn, g_state)
            # g_extractor.pipe(batch)
            #
            # # run by genre's changes
            # p_state = State(ModelNames.PERSON)
            # p_extractor = extract_utils.FilmworkExtractorByPerson(conn, p_state)
            # p_extractor.pipe(batch)

    @coroutine
    def transform(self, batch: Generator) -> Generator[None, DictRow, None]:
        while records := (yield):
            bulk = transform_utils.agregate_films(records)

            batch.send(bulk)

    @backoff.on_exception(backoff.expo,
                          elastic_transport.ConnectionError,
                          on_giveup=_giveup,
                          on_backoff=_backoff,
                          max_tries=5,
                          jitter=backoff.random_jitter,
                          max_time=16)
    @coroutine
    def load(self) -> Generator[None, Tuple, None]:

        with Elasticsearch(config.ELASTIC_CONNECT) as client:
            if not client.ping():
                raise elastic_transport.ConnectionError("Failed check connections")
            while subjects := (yield):
                resp = helpers.bulk(client=client, actions=subjects)
                logger.info(f"{resp[0]} documents has been updated.")


if __name__ == "__main__":
    etl = Etl('filmwork')

    unloads = etl.load()
    multiplication = etl.transform(unloads)
    etl.extract(multiplication)