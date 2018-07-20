FROM python:3.6-alpine

RUN apk add --no-cache alpine-sdk zeromq-dev && \
    apk add --no-cache --allow-untrusted \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/community \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/main \
    --repository http://nl.alpinelinux.org/alpine/edge/community \
    git-lfs && \
    git lfs install

COPY . /code/renku
WORKDIR /code/renku
RUN pip install --no-cache -e .[all]

WORKDIR /
