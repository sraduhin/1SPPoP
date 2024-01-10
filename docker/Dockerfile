FROM python:3.10

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR opt/app

RUN pip install --upgrade pip

COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./docker/app/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
