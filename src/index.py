import argparse

from elasticsearch import Elasticsearch

from core import config
from core.elastic import MAPPING_SCHEME
from state import State


class Index:
    mapping = MAPPING_SCHEME

    def __init__(self, index_name, client):
        self.index_name = index_name
        self.client = client

    def build(self, force=False):
        if self._is_exists():
            if force:
                self._delete()
            else:
                return None  # Index already exists
        self.client.indices.create(index=self.index_name, **self.mapping)
        State.set_default()

    def _delete(self):
        self.client.indices.delete(index=self.index_name)

    def _is_exists(self):
        check_index = self.client.indices.exists(index=self.index_name)
        if check_index.body:
            return True


def build_index():
    parser = argparse.ArgumentParser(
        description="Runnings before etl to check/build or rebuild ES index"
    )
    parser.add_argument('-f', '--force',
                        help="force rebuild existing index", default=False)
    args = parser.parse_args()

    with Elasticsearch(config.ELASTIC_CONNECT) as client:
        index = Index(config.ELASTIC_INDEX_NAME, client)
        index.build(args.force)


if __name__ == "__main__":
    build_index()
