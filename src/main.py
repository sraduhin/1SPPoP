import psycopg2


from core.config import DB_CONNECT
from models import ModelTableNames
from process import Extract
from state import State

CHUNK_SIZE = 1000


def run_etl():

    # with Elasticsearch(config.ELASTIC_HOST) as client:
    with psycopg2.connect(**DB_CONNECT) as conn:
        filmwork_state = State(ModelTableNames.FILMWORK).get_state()
        genre_state = State(ModelTableNames.GENRE).get_state()
        person_state = State(ModelTableNames.PERSON).get_state()

        filmwork_extractor = Extract(ModelTableNames.FILMWORK, conn)
        if not filmwork_state:
            pass

        person_extractor = Extract(ModelTableNames.PERSON, conn)
        persons = person_extractor.get_data(person_state)
        while persons:
            films = filmwork_extractor.



if __name__ == "__main__":
    run_etl()