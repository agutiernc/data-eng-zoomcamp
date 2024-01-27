FROM python:3.10

RUN pip install pandas

# location in the image container where we copy the file
# does "cd /app" and will show current directory when ran
WORKDIR  /app

# first from local source -- then destination
COPY pipeline.py pipeline.py

# runs python then script
ENTRYPOINT ["python", "pipeline.py"]