import orjson

from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel


class ModelTableNames:
    FILMWORK = 'filmwork'
    GENRE = 'genre'
    PERSON = 'person'

class RedisTimeCache:
    persons: datetime
    genres: datetime
    filmworks: datetime

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps


class Cache2(BaseModel):
    persons: datetime
    genres: datetime
    filmworks: datetime

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps