FROM python:3.6-alpine

COPY . /code/renga
WORKDIR /code/renga
RUN pip install --no-cache /code/renga
RUN pip install --no-cache cwlref-runner

WORKDIR /
