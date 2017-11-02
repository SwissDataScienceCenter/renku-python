FROM jupyter/minimal-notebook:latest

COPY . /code/renga
RUN pip install --no-cache /code/renga
