import argparse

from elasticsearch import Elasticsearch

from core import config
from state import State
from config import Models, IndexConfig


class Index:

    def __init__(self, client: Elasticsearch, **kwargs):
        model = kwargs.get("model")
        self.config = IndexConfig(model=model)
        self.client = client

    def build(self, force=False):
        if self._is_exists():
            if force:
                self._delete()
            else:
                return None  # Index already exists
        self.client.indices.create(
            index=self.config.index, **self.config.mapping
        )
        State.set_default()

    def _delete(self):
        self.client.indices.delete(index=self.config.index)

    def _is_exists(self):
        check_index = self.client.indices.exists(index=self.config.index)
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
        filmwork_index = Index(client, model=Models.FILMWORK)
        filmwork_index.build(args.force)

        genre_index = Index(client, model=Models.GENRE)
        genre_index.build(args.force)

        person_index = Index(client, model=Models.PERSON)
        person_index.build(args.force)


if __name__ == "__main__":
    build_index()
