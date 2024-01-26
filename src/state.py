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

    def push_to_queue(self, key, new_ids):
        self.redis.rpush(key, orjson.dumps(new_ids))

    def get_from_queue(self, key, start=0, end=0):
        data = self.redis.lrange(key, start, end)
        if data:
            return orjson.loads(data[0])
        return None

    def pop_from_queue(self, key):
        return self.redis.lpop(key)

    def flash_state(self):
        self.redis.flushdb()


class State:
    PREFIX = "STATE:"
    host = config.REDIS_HOST
    port = config.REDIS_PORT
    storage = RedisStorage(redis=Redis(host=host, port=port))

    def __init__(self, key: str):
        self.key = self.PREFIX + key

    def get_state(self):
        return self.storage.retrieve_state(self.key)

    def set_state(self, new_value):
        self.storage.save_state(self.key, new_value)

    @classmethod
    def set_default(cls):
        cls.storage.flash_state()


class Queue(State):
    PREFIX = "QUEUE:"

    def add_to_queue(self, other):
        self.storage.push_to_queue(self.key, other)

    def get_from_queue(self):
        return self.storage.get_from_queue(self.key)

    def pop_from_queue(self):
        return self.storage.pop_from_queue(self.key)


from collections import deque

dq = deque()

