import backoff
import psycopg2

from typing import Generator, List, Any
from psycopg2.extras import DictCursor

from core.config import logger, DB_CONNECT

from enums import Model, Models
from state import State, Queue
from utils.backoff import _giveup, _backoff


class BaseExtractor:
    CHUNK_SIZE = 100

    def __init__(self, model: Models, related=None):
        redis_key = Model(model).state
        self.state = State(redis_key)
        self.model = model
        self.current_state = self.state.get_state()
        self.queries = Model(model).query
        self.connect = psycopg2.connect(**DB_CONNECT)
        self.related = related + "_" + model if related else None
        if self.related:
            self.stack = Queue(self.related)

    def _add_to_queue(self, objects):
        object_list = [_[0] for _ in objects]
        self.stack.add_to_queue(object_list)

    def _log_this(self, num: int):
        logger.info(f"Handled {num} new {self.model}s. Checking index...")

    def _log_nothing_to_load(self):
        logger.info(f"Nothing to extract in {self.model}s")

    def _make_query(self):
        query = {}
        if self.current_state:
            query["query"] = self.queries["modified"]
            query["vars"] = (self.current_state,)
        else:
            query["query"] = self.queries["all"]
        return query

    def _fill_missed_data(self, objects: List[Any], connect: psycopg2.connect):
        id_list = [_[0] for _ in objects]
        with connect.cursor(cursor_factory=DictCursor) as inner_cursor:
            inner_cursor.execute(self.queries["other"], (tuple(id_list),))
            return inner_cursor.fetchall()

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        on_giveup=_giveup,
        on_backoff=_backoff,
        max_tries=5,
        jitter=backoff.random_jitter,
        max_time=16,
    )
    def pipe(self, batch: Generator):
        query = self._make_query()
        if query:
            with self.connect.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(**query)
                objects = cursor.fetchmany(self.CHUNK_SIZE)
                while objects:
                    self._log_this(len(objects))
                    required_data = self._fill_missed_data(objects, self.connect)
                    batch.send(required_data)

                    new_state = objects[-1][-1]  # modified
                    self.state.set_state(new_state)

                    if self.related:
                        self._add_to_queue(objects)
                    objects = cursor.fetchmany(self.CHUNK_SIZE)

        self._log_nothing_to_load()



class SecondaryExtractor(BaseExtractor):

    def __init__(self, model: Models, submodel: Models):
        super().__init__(model)
        redis_key = Model(model) + Model(submodel)
        self.submodel = submodel
        self.state = State(redis_key)
        self.queue = Queue(redis_key)
        self.current_state = self.state.get_state()
        self.queries = Model(model).query
        self.connect = psycopg2.connect(**DB_CONNECT)

    def _get_from_queue(self):
        return self.queue.get_from_queue()

    def _log_this(self, num: int):
        logger.info(f"Handled {num} updates in {self.model}s by {self.submodel}. Checking index...")

    def _log_nothing_to_load(self):
        pass

    def _make_query(self):
        vars_ = self._get_from_queue()
        if vars_:
            return {
                "query": self.queries[f"by_{self.submodel}"],
                "vars": (tuple(vars_),),
            }
        return None

    def pipe(self, batch: Generator):
        data = self._get_from_queue()
        while data:
            super().pipe(batch)
            data = self.queue.pop_from_queue()
        logger.info(f"Nothing to extract in {self.model}s")
