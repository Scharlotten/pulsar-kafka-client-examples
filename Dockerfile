FROM python:3.10-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

ENV PYTHONUNBUFFERED 1

COPY *.py ./
