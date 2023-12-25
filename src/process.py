from datetime import datetime

import psycopg2

from models import ModelTableNames
from state import State


class Extract:
    CHUNK_SIZE = 100

    def __init__(self, model: str, conn: psycopg2.connect):
        self.model = model
        self.conn = conn

    def get_data(self, modified=None):
        cursor = self.conn.cursor()

        raw_context = Context(self.model)
        raw = get_raw(modified)
        cursor.execute(raw)
        return cursor.fetchany()


class Context:
    def __init__(self, model: str):
        self.model = model

    def get_persons(self, modified):
        raw = "SELECT id, modified" \
              "FROM content.person" \
              f"WHERE modified > {modified}" \
              f"ORDER BY modified" \
              f"LIMIT 100"

def collect_data(model: str):
    CHUNK_SIZE = 100



    def get_raw(self, modified=None) -> str:
        if self.model == ModelTableNames.FILMWORK:

        raw = f"SELECT id, modified FROM content.{self.model}"
        if modified:
            raw += f" WHERE modified > '{modified}'"
        raw += f" ORDER BY modified"
        return raw

