from typing import Optional

import orjson
from redis import Redis


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
    storage = RedisStorage(redis=Redis())

    def __init__(self, key):
        self.key = key

    def get_state(self):
        return self.storage.retrieve_state(self.key)

    def set_state(self, new_value):
        self.storage.save_state(self.key, new_value)

    @classmethod
    def set_default(cls):
        cls.storage.flash_state()
