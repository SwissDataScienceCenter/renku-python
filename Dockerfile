FROM python:3.9-slim as base

# hadolint ignore=DL3008,DL3009,DL3013
RUN apt-get update && \
    apt-get install --no-install-recommends -y git git-lfs=2.* python3-dev tini bash curl && \
    pip install --no-cache-dir --upgrade pip

FROM base as builder

# hadolint ignore=DL3008
RUN apt-get install --no-install-recommends -y build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# The python install is done in two steps to avoid re-installing all dependencies every
# time the code changes
# set the BUILD_CORE_SERVICE to non null to install additional service dependencies
ARG BUILD_CORE_SERVICE
COPY pyproject.toml poetry.lock README.rst CHANGES.rst Makefile /code/renku/
COPY .git /code/renku/.git
COPY renku /code/renku/renku
WORKDIR /code/renku

# Set CLEAN_INSTALL to a non-null value to ensure that only a committed version of
# renku-python is installed in the image. This is the default for chartpress builds.
ARG CLEAN_INSTALL
RUN if [ -n "${CLEAN_INSTALL}" ]; then git reset --hard ; fi && \
    make download-templates

ENV POETRY_HOME=/opt/poetry
# hadolint ignore=DL4006
RUN if [ -n "${BUILD_CORE_SERVICE}" ]; then export EXT_BUILD="-E service" ; fi && \
    mkdir -p /opt/poetry && \
    curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.3.1 python3 - && \
    pip3 install --no-cache-dir poetry-dynamic-versioning==0.21.2 && \
    /opt/poetry/bin/poetry config virtualenvs.in-project true  && \
    /opt/poetry/bin/poetry config virtualenvs.options.no-setuptools true && \
    /opt/poetry/bin/poetry config virtualenvs.options.no-pip true  && \
    /opt/poetry/bin/poetry self add "poetry-dynamic-versioning[plugin]" && \
    /opt/poetry/bin/poetry install "${EXT_BUILD}" -E toil

FROM base
ARG BUILD_CORE_SERVICE
RUN addgroup -gid 1000 shuhitsu && \
    useradd -m -u 1000 -g shuhitsu shuhitsu && \
    git lfs install && \
    if [ -n "${BUILD_CORE_SERVICE}" ]; then mkdir /svc && chown shuhitsu:shuhitsu /svc ; fi

COPY --from=builder /code/renku /code/renku
ENV PATH="${PATH}:/code/renku/.venv/bin"

# shuhitsu (執筆): The "secretary" of the renga, as it were, who is responsible for
# writing down renga verses and for the proceedings of the renga.
USER shuhitsu

ENV RENKU_SVC_NUM_WORKERS 4
ENV RENKU_SVC_NUM_THREADS 8
ENV RENKU_DISABLE_VERSION_CHECK=1

ENTRYPOINT ["tini", "-g", "--", "renku"]
