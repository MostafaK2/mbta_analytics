FROM python:3.12-slim

WORKDIR /app/repo

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ingestion ./ingestion