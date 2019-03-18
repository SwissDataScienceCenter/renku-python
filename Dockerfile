FROM python:3.6-alpine as base

RUN apk add --no-cache git && \
    pip install --no-cache --upgrade pip

FROM base as builder

ENV BUILD_DEPS "alpine-sdk g++ gcc linux-headers libxslt-dev zeromq-dev"
RUN apk add --no-cache $BUILD_DEPS

COPY . /code/renku
WORKDIR /code/renku

RUN pip wheel --wheel-dir /wheels . && \
    pip install --no-index --no-warn-script-location --root=/pythonroot/ /wheels/*.whl && \
    apk del --purge $BUILD_DEPS

FROM base

RUN apk add --no-cache --allow-untrusted \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/community \
    --repository http://dl-cdn.alpinelinux.org/alpine/latest-stable/main \
    --repository http://nl.alpinelinux.org/alpine/edge/community \
    git-lfs && \
    git lfs install

COPY --from=builder /pythonroot/ /

CMD ["renku"]
