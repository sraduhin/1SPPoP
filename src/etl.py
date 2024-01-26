import backoff
import elastic_transport

from collections.abc import Generator
from functools import wraps
from typing import Tuple, Any

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow

from core import config
from core.config import logger

from enums import Models
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


class BaseEtl:
    def __init__(self):
        self.extractor = None
        self.transformer = None

    def _extract(self, batch: Generator) -> None:
        self.extractor.pipe(batch)

    @coroutine
    def _transform(self, batch: Generator) -> Generator[None, DictRow, None]:
        while records := (yield):
            bulk = self.transformer.aggregate(records)

            batch.send(bulk)

    @backoff.on_exception(
        backoff.expo,
        elastic_transport.ConnectionError,
        on_giveup=_giveup,
        on_backoff=_backoff,
        max_tries=5,
        jitter=backoff.random_jitter,
        max_time=16,
    )
    @coroutine
    def _load(self) -> Generator[None, Tuple, None]:
        with Elasticsearch(config.ELASTIC_CONNECT) as client:
            if not client.ping():
                raise elastic_transport.ConnectionError("Failed check connections")
            while subjects := (yield):
                resp = helpers.bulk(client=client, actions=subjects)
                logger.info(f"{resp[0]} documents has been updated.")

    def run(self):
        unloads = self._load()
        multiplication = self._transform(unloads)
        self._extract(multiplication)


class FWETL(BaseEtl):
    def __init__(self, model: Models):
        super().__init__()
        self.extractor = extract_utils.BaseExtractor(model)
        self.transformer = transform_utils.FWTransformer()


class GenreETL(BaseEtl):
    def __init__(self, model: Models):
        super().__init__()
        self.extractor = extract_utils.BaseExtractor(model, Models.FILMWORK)
        self.transformer = transform_utils.GTransformer()


class PersonETL(BaseEtl):
    def __init__(self, model: Models):
        super().__init__()
        self.extractor = extract_utils.BaseExtractor(model, Models.FILMWORK)
        self.transformer = transform_utils.PTransformer()


class FWETLByModel(FWETL):

    def __init__(self, model: Models, submodel: Models):
        super().__init__(model)
        self.extractor = extract_utils.SecondaryExtractor(model, submodel)


if __name__ == "__main__":
    """
    The idea of pipeline has been respectfully stolen from
    https://github.com/s-klimov/etl-template/tree/1-base
    """
    etl = FWETLByModel(Models.FILMWORK, Models.PERSON)
    etl.run()




