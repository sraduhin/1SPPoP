import queryes
from core import elastic


class Models:
    FILMWORK: str = "filmwork"
    GENRE: str = "genre"
    PERSON: str = "person"


CONF = {
    "index": {
        Models.FILMWORK: "movies",
        Models.GENRE: "genres",
        Models.PERSON: "persons",
    },
    "state": {
        Models.FILMWORK: "movies",
        Models.GENRE: "genres",
        Models.PERSON: "persons",
    },
    "mapping": {
        Models.FILMWORK: elastic.FILMWORK_MAPPING_SCHEME,
        Models.GENRE: elastic.GENRE_MAPPING_SCHEME,
        Models.PERSON: elastic.PERSON_MAPPING_SCHEME,
    },
    "queries": {
        Models.FILMWORK: (queryes.FILMWORKS, queryes.FILMWORKS_ALL),
        Models.GENRE: (queryes.GENRES, queryes.GENRES_ALL),
        Models.PERSON: (queryes.PERSONS, queryes.PERSONS_ALL),
        "extra_query": {
            Models.GENRE: queryes.FILMWORKS_BY_G,
            Models.PERSON: queryes.FILMWORKS_BY_P,
        }
    }
}


class ETLConfig:

    def __init__(self, **kwargs):
        model = kwargs.get("model")
        self.index = CONF["index"][model]


class IndexConfig:

    def __init__(self, **kwargs):
        model = kwargs.get("model")
        self.index = CONF["index"][model]
        self.mapping = CONF["mapping"][model]


class ExtractorConfig:

    def __init__(self, **kwargs):
        self.model = kwargs.get("model")
        self.submodel = kwargs.get("submodel", None)
        self.queries = CONF["queries"][self.model]
        self.state = CONF["state"][self.model].upper()
        if self.submodel:
            self.queries = CONF["queries"][self.submodel]
            self.extra_query = CONF["queries"]["extra_query"][self.submodel]
            self.state += "_" + CONF["state"][self.submodel]
