FROM python:3.10

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# location in the image container where we copy the file
# does "cd /app" and will show current directory when ran
WORKDIR  /app

# first from local source -- then destination
COPY ingest_data.py ingest_data.py

# runs python then script
ENTRYPOINT ["python", "ingest_data.py"]


# ===== BEFORE video 1.2.4 =============
# FROM python:3.10

# RUN pip install pandas

# # location in the image container where we copy the file
# # does "cd /app" and will show current directory when ran
# WORKDIR  /app

# # first from local source -- then destination
# COPY pipeline.py pipeline.py

# # runs python then script
# ENTRYPOINT ["python", "pipeline.py"]