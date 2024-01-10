#!/bin/bash

echo " >>>> (Re)building index"
python3 index.py

echo " >>>> Running a scheduled worker"
celery -A tasks worker -B --loglevel=info