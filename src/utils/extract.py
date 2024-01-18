import psycopg2
import queryes

from typing import Generator, List
from psycopg2.extras import DictCursor

from core.config import logger
from config import Models, ExtractorConfig
from state import State
from core.config import DB_CONNECT


class BaseExtractor:
    CHUNK_SIZE = 100

    def __init__(self, **kwargs):
        model = kwargs.get("model")
        submodel = kwargs.get("submodel", None)
        self.config = ExtractorConfig(model=model, submodel=submodel)
        self.state = State(self.config.state)

    def _sql(self, current_state):
        # returns sql with 0) 'modified' or 1) all
        return self.config.queries[0] if current_state else self.config.queries[1]

    def _log_this(self, num: int):
        logger.info(f"Handled {num} new {self.config.model}s. Checking index...")

    def pipe(
            self, batch: Generator, connect: psycopg2.connect
    ):
        current_state = self.state.get_state()
        with connect.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(self._sql(current_state), (current_state,))
            objects = cursor.fetchmany(self.CHUNK_SIZE)
            while objects:
                self._log_this(len(objects))
                batch.send(objects)

                new_state = objects[-1][-1]  # modified
                self.state.set_state(new_state)

                objects = cursor.fetchmany(self.CHUNK_SIZE)


class FWExtractor(BaseExtractor):

    def _get_relevant_date(self, objects: list, connect: psycopg2.connect):
        id_list = [_[0] for _ in objects]
        with connect.cursor(cursor_factory=DictCursor) as inner_cursor:
            inner_cursor.execute(queryes.MISSING_DATA, (tuple(id_list),))
            return inner_cursor.fetchall()

    def pipe(
            self, batch: Generator, connect: psycopg2.connect
    ):
        current_state = self.state.get_state()
        with connect.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(self._sql(current_state), (current_state,))
            objects = cursor.fetchmany(self.CHUNK_SIZE)
            while objects:
                self._log_this(len(objects))
                data = self._get_relevant_date(objects, connect)

                batch.send(data)

                new_state = objects[-1][-1]  # modified
                self.state.set_state(new_state)

                objects = cursor.fetchmany(self.CHUNK_SIZE)


class FWEXtractorBy(FWExtractor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _extra_sql(self):
        return self.config.extra_query

    def _log_that(self, num: int):
        logger.info(f"Handled {num} new {self.config.submodel}s. Passing through...")

    def _get_intermediate_data(self, objects: list, connect: psycopg2.connect):
        id_list = [_[0] for _ in objects]
        with connect.cursor(cursor_factory=DictCursor) as inner_cursor:
            inner_cursor.execute(self._extra_sql(), (tuple(id_list),))  # get films
            return inner_cursor.fetchmany(self.CHUNK_SIZE)

    def pipe(self, batch: Generator, connect):
        current_state = self.state.get_state()
        with connect.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(self._sql(current_state), (current_state,))  # get submodel
            objects = cursor.fetchmany(self.CHUNK_SIZE)
            while objects:
                self._log_that(len(objects))

                id_list = [_[0] for _ in objects]
                with connect.cursor(cursor_factory=DictCursor) as inner_cursor:
                    inner_cursor.execute(self._extra_sql(), (tuple(id_list),))
                    im_data = inner_cursor.fetchmany(self.CHUNK_SIZE)
                    while im_data:
                        self._log_this(len(im_data))
                        data = self._get_relevant_date(objects, connect)
                        if data:
                            batch.send(data)
                        im_data = inner_cursor.fetchmany(self.CHUNK_SIZE)

                new_state = objects[-1][-1]  # modified
                self.state.set_state(new_state)

                objects = cursor.fetchmany(self.CHUNK_SIZE)


class CombinedExtractor:

    def __init__(self, **kwargs):
        model = kwargs.get("model")
        self.config = ExtractorConfig(model=model)
        self.fw_extractor = FWExtractor(model=model)
        self.fwg_extractor = FWEXtractorBy(model=model, submodel=Models.GENRE)
        self.fwp_extractor = FWEXtractorBy(model=model, submodel=Models.PERSON)

    def pipe(self, batch, connect: psycopg2.connect):
        self.fw_extractor.pipe(batch, connect)
        self.fwg_extractor.pipe(batch, connect)
        self.fwp_extractor.pipe(batch, connect)
