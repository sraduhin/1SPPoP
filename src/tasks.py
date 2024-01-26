from celery import Celery

from enums import Models
from etl import FWETL, FWETLByModel, GenreETL, PersonETL


app = Celery("tasks")
app.config_from_object("core.config", namespace="CELERY")


@app.task
def fw_pipeline():
    etl = FWETL(Models.FILMWORK)
    etl.run()


@app.task
def genre_pipeline():
    etl = GenreETL(Models.GENRE)
    etl.run()


@app.task
def person_pipeline():
    etl = PersonETL(Models.PERSON)
    etl.run()


@app.task
def fw_by_genre_pipeline():
    etl = FWETLByModel(Models.FILMWORK, Models.GENRE)
    etl.run()


@app.task
def fw_by_person_pipeline():
    etl = FWETLByModel(Models.FILMWORK, Models.PERSON)
    etl.run()


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10.0, fw_pipeline.s())
    sender.add_periodic_task(10.0, genre_pipeline.s())
    sender.add_periodic_task(10.0, person_pipeline.s())
    sender.add_periodic_task(10.0, fw_by_genre_pipeline.s())
    sender.add_periodic_task(10.0, fw_by_person_pipeline.s())
