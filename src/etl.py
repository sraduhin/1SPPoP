from collections.abc import Generator
from datetime import datetime
from functools import wraps
from typing import Tuple, Dict, Any

import psycopg2
from psycopg2.extras import DictCursor, DictRow

from core import config
from core.config import DB_CONNECT
import queryes as sql
from elasticsearch import Elasticsearch, helpers
from utils import transorm as transform_utils


def coroutine(func):
    @wraps(func)
    def inner(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Generator:
        fn: Generator = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def extract(batch: Generator) -> None:
    CHUNK_SIZE = 1000

    with psycopg2.connect(**DB_CONNECT) as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:

            # поиск просто по фильмам
            cursor.execute(sql.FILMWORKS)
            filmworks = cursor.fetchmany(CHUNK_SIZE)
            while filmworks:
                filmwork_ids = [filmwork[0] for filmwork in filmworks]
                cursor.execute(sql.MISSING_DATA, (tuple(filmwork_ids),))
                data = cursor.fetchall()
                batch.send(data)
                filmworks = cursor.fetchmany(CHUNK_SIZE)

            # поиск по создателям
            cursor.execute(sql.PERSONS)  # добавить плейсхолдер
            persons = cursor.fetchmany(CHUNK_SIZE)
            while persons:
                persons_ids = [person[0] for person in persons]
                persons = cursor.fetchmany(CHUNK_SIZE)
                cursor.execute(sql.FILMWORKS_BY_P, (tuple(persons_ids),))  # works with tuple, not a list. Dont ask why
                filmworks = cursor.fetchmany(CHUNK_SIZE)
                while filmworks:
                    filmwork_ids = [filmwork[0] for filmwork in filmworks]
                    cursor.execute(sql.MISSING_DATA, (tuple(filmwork_ids),))
                    data = cursor.fetchall()
                    batch.send(data)
                    filmworks = cursor.fetchmany(CHUNK_SIZE)

            # поиск по жанрам
            cursor.execute(sql.GENRES)
            genres = cursor.fetchmany(CHUNK_SIZE)
            while genres:
                genres_ids = [genre[0] for genre in genres]
                genres = cursor.fetchmany(CHUNK_SIZE)
                cursor.execute(sql.FILMWORKS_BY_G, (tuple(genres_ids),))  # works with tuple, not a list. Dont ask why
                filmworks = cursor.fetchmany(CHUNK_SIZE)
                while filmworks:
                    filmwork_ids = [filmwork[0] for filmwork in filmworks]
                    cursor.execute(sql.MISSING_DATA, (tuple(filmwork_ids),))
                    data = cursor.fetchall()
                    batch.send(data)
                    filmworks = cursor.fetchmany(CHUNK_SIZE)


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
            print(f"{resp[0]} documents has been loaded.")


def main():
    """
        The idea of pipeline has been respectfully stolen from
        https://github.com/s-klimov/etl-template/tree/1-base
    """
    unloads = load()
    multiplication = transform(unloads)
    extract(multiplication)


if __name__ == '__main__':
    main()
