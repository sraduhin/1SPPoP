from celery import Celery
from celery.schedules import crontab

from main import main


app = Celery("tasks")
app.config_from_object("core.config", namespace="CELERY")


@app.task
def etl_pipeline():
    main()


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute="*/1"), etl_pipeline.s())
