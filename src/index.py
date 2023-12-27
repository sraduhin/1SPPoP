from core import config
from core.elastic import MAPPING_SCHEME
from core.config import logger
from elasticsearch import Elasticsearch


class Index:
    mapping = MAPPING_SCHEME

    def __init__(self, index_name, client):
        self.index_name = index_name
        self.client = client

    def build(self, force=False):
        if self._is_exists():
            if force:
                self._delete()
        client.indices.create(index=self.index_name, **self.mapping)
        # State.set_default()

    def _delete(self):
        self.client.indices.delete(index=self.index_name)

    def _is_exists(self):
        check_index = self.client.indices.exists(index=self.index_name)
        if check_index.body:
            return True


if __name__ == "__main__":
    with Elasticsearch(config.ELASTIC_CONNECT) as client:
        index = Index("movies", client)
        index.build(force=True)
