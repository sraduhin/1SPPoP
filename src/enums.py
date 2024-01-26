import queryes
from core import elastic


class Models:
    FILMWORK = "filmwork"
    GENRE = "genre"
    PERSON = "person"


class Model:

    def __init__(self, model):
        self.validate(model)
        self.model = model

    def __add__(self, other):
        return self.model + "_" + other.model

    @classmethod
    def validate(cls, model):
        if model not in Models.__dict__.values():
            raise ValueError("Unknown data model")

    @property
    def index(self):
        return {
            Models.FILMWORK: 'movies',
            Models.GENRE: 'genres',
            Models.PERSON: 'persons',
        }.get(self.model)

    @property
    def state(self):
        return {
            Models.FILMWORK: 'movies',
            Models.GENRE: 'genres',
            Models.PERSON: 'persons',
        }.get(self.model)

    @property
    def query(self):
        return {
            Models.FILMWORK: {
                'modified': queryes.FILMWORKS_BY_MODIFIED,
                'all': queryes.FILMWORKS_ALL,
                'other': queryes.FILMWORK_MISSING_DATA,
                'by_genre': queryes.FILMWORKS_BY_G,
                'by_person': queryes.FILMWORKS_BY_P,
            },
            Models.GENRE: {
                'modified': queryes.GENRES_BY_MODIFIED,
                'all': queryes.GENRES_ALL,
                'other': queryes.GENRES_MISSING_DATA,
            },
            Models.PERSON: {
                'modified': queryes.PERSONS_BY_MODIFIED,
                'all': queryes.PERSONS_ALL,
                'other': queryes.PERSONS_MISSING_DATA,
            },
        }.get(self.model)

    @property
    def elastic_mapping(self):
        return {
            Models.FILMWORK: elastic.FILMWORK_MAPPING_SCHEME,
            Models.GENRE: elastic.GENRE_MAPPING_SCHEME,
            Models.PERSON: elastic.PERSON_MAPPING_SCHEME,
        }.get(self.model)

