FROM python:3.6-alpine

ENV BUILD_DEPS "alpine-sdk g++ gcc linux-headers libxslt-dev zeromq-dev"

RUN apk add --no-cache $BUILD_DEPS && \
    apk add --no-cache --allow-untrusted \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/community \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/main \
    --repository http://nl.alpinelinux.org/alpine/edge/community \
    git-lfs && \
    git lfs install

COPY . /code/renku
WORKDIR /code/renku
RUN pip install --no-cache -e .[all]

RUN apk del --purge $BUILD_DEPS

WORKDIR /
