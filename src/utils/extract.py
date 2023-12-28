import psycopg2
import queryes

from typing import Generator
from psycopg2.extras import DictCursor

from core.config import logger
from schemas import ModelNames


class FilmworkExtractor:
    CHUNK_SIZE = 1000
    model = ModelNames.FILMWORK

    def __init__(self, connect: psycopg2.connect, state):
        self.connect = connect
        self.state = state

    def _sql(self, current_state):
        return queryes.FILMWORKS if current_state else queryes.FILMWORKS_ALL

    def _send(self, objects: list, batch: Generator):
        id_list = [obj[0] for obj in objects]
        with self.connect.cursor(cursor_factory=DictCursor) as inner_cursor:
            inner_cursor.execute(queryes.MISSING_DATA, (tuple(id_list),))
            data = inner_cursor.fetchall()
        batch.send(data)

    def _log_this(self, num: int):
        logger.info(f"Found {num} new {self.model}s. Updating...")

    def pipe(self, batch: Generator):
        current_state = self.state.get_state()
        with self.connect.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(self._sql(current_state), (current_state,))
            objects = cursor.fetchmany(self.CHUNK_SIZE)
            while objects:
                self._log_this(len(objects))
                self._send(objects, batch)

                new_state = objects[-1][1]  # modified
                self.state.set_state(new_state)

                objects = cursor.fetchmany(self.CHUNK_SIZE)


class FilmworkExtractorByPerson(FilmworkExtractor):
    model = ModelNames.PERSON

    def _sql(self, current_state):
        return queryes.PERSONS if current_state else queryes.PERSONS_ALL

    def _sql_films(self):
        return queryes.FILMWORKS_BY_P

    def pipe(self, batch: Generator):
        current_state = self.state.get_state()
        with self.connect.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(self._sql(current_state), (current_state,))  # get persons
            objects = cursor.fetchmany(self.CHUNK_SIZE)
            while objects:
                self._log_this(len(objects))

                ids = [object[0] for object in objects]

                with self.connect.cursor(cursor_factory=DictCursor) as inner_cursor:
                    # works with tuple, not a list. Don't ask why
                    inner_cursor.execute(  # get films by changed persons
                        self._sql_films(), (tuple(ids),)
                    )
                    inner_objects = inner_cursor.fetchmany(self.CHUNK_SIZE)

                    while inner_objects:
                        self._send(inner_objects, batch)
                        inner_objects = inner_cursor.fetchmany(self.CHUNK_SIZE)

                new_state = objects[-1][1]  # modified
                self.state.set_state(new_state)

                objects = cursor.fetchmany(self.CHUNK_SIZE)


class FilmworkExtractorByGenre(FilmworkExtractorByPerson):
    model = ModelNames.GENRE

    def _sql(self, current_state):
        return queryes.GENRES if current_state else queryes.GENRES_ALL

    def _sql_films(self):
        return queryes.FILMWORKS_BY_G
