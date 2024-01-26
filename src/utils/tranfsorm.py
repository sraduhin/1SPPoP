from typing import Any, List

from enums import Models, Model


def append_person(bulk: list, person: dict) -> None:
    role = person.pop("role")
    name = person.get("name")
    match role:
        case "actor":
            bulk[-1]["_source"]["actors"].append(person)
            bulk[-1]["_source"]["actors_names"].append(name)
        case "director":
            bulk[-1]["_source"]["director"].append(name)
        case "writer":
            bulk[-1]["_source"]["writers"].append(person)
            bulk[-1]["_source"]["writers_names"].append(name)


def append_genre(bulk: list, genre: str) -> None:
    bulk[-1]["_source"]["genre"].append(genre)


class BaseTransformer:

    def aggregate(self, records: List[Any]):
        pass


class FWTransformer(BaseTransformer):
    INDEX = Model(Models.FILMWORK).index

    def aggregate(self, records: List[Any]) -> list:
        bulk = []
        current_fw_id = ""
        current_g_name = ""
        persons_collected = False
        for item in records:
            fw_id = item[0]
            g_name = item[10]
            pfw_role = item[7]
            if fw_id == current_fw_id:
                if g_name == current_g_name:
                    if persons_collected:
                        continue
                    person = {"id": item[8], "name": item[9], "role": pfw_role}
                    append_person(bulk, person)

                else:
                    append_genre(bulk, g_name)
                    persons_collected = True
                    current_g_name = g_name
            else:
                bulk.append(
                    {
                        "_index": self.INDEX,
                        "_op_type": "index",
                        "_id": fw_id,
                        "_source": {
                            "id": fw_id,
                            "title": item[1],
                            "description": item[2],
                            "imdb_rating": item[3],
                            "actors": [],
                            "actors_names": [],
                            "director": [],
                            "writers": [],
                            "writers_names": [],
                            "genre": [],
                        },
                    }
                )
                current_fw_id = fw_id
                person = {"id": item[8], "name": item[9], "role": pfw_role}
                append_person(bulk, person)
                append_genre(bulk, g_name)
                current_g_name = g_name
                persons_collected = False

        return bulk


class GTransformer(BaseTransformer):
    INDEX = Model(Models.GENRE).index

    def aggregate(self, records: List[Any]) -> list:
        bulk = []
        for item in records:
            bulk.append(
                {
                    "_index": self.INDEX,
                    "_op_type": "index",
                    "_id": item[0],
                    "_source": {
                        "id": item[0],
                        "title": item[1],
                        "description": item[2],
                    },
                }
            )

        return bulk


class PTransformer(BaseTransformer):
    INDEX = Model(Models.PERSON).index

    def aggregate(self, records: List[Any]) -> list:
        bulk = []
        for item in records:
            bulk.append(
                {
                    "_index": self.INDEX,
                    "_op_type": "index",
                    "_id": item[0],
                    "_source": {
                        "id": item[0],
                        "name": item[1],
                        "male": item[2],
                    },
                }
            )

        return bulk
