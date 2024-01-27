FROM python:3.10

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR  /app

# first from local source -- then destination
COPY ingest_data.py ingest_data.py

# runs python then script
ENTRYPOINT ["python", "ingest_data.py"]