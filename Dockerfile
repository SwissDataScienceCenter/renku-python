FROM python:3.6-alpine as base

RUN apk add --no-cache git

FROM base as builder

COPY . /code/renku
WORKDIR /code/renku

RUN python setup.py bdist_wheel

RUN apk del --purge $BUILD_DEPS

FROM base
ENV BUILD_DEPS "alpine-sdk g++ gcc linux-headers libxslt-dev zeromq-dev"

RUN apk add --no-cache $BUILD_DEPS && \
    apk add --no-cache --allow-untrusted \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/community \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/main \
    --repository http://nl.alpinelinux.org/alpine/edge/community \
    git-lfs && \
    git lfs install

COPY --from=builder /code/renku/dist /install

RUN pip install --no-cache --upgrade pip && \
    pip install --no-cache /install/renku-*.whl && \
    rm /install/renku-*.whl

CMD ["renku"]
