from typing import Optional

import orjson
from redis import Redis

from core import config


class RedisStorage:
    def __init__(self, redis: Redis):
        self.redis = redis

    def retrieve_state(self, key) -> Optional[dict]:
        state = self.redis.get(key)
        if state:
            return orjson.loads(state)
        return None

    def save_state(self, key, new_value):
        self.redis.set(key, orjson.dumps(new_value))

    def flash_state(self):
        self.redis.flushdb()


class State:
    host = config.REDIS_HOST
    port = config.REDIS_PORT
    storage = RedisStorage(redis=Redis(host=host, port=port))

    def __init__(self, key):
        self.key = key

    def get_state(self):
        return self.storage.retrieve_state(self.key)

    def set_state(self, new_value):
        self.storage.save_state(self.key, new_value)

    @classmethod
    def set_default(cls):
        cls.storage.flash_state()
