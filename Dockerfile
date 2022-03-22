FROM python:3.9-slim as base

RUN apt-get update && \
    apt-get install -y git git-lfs=2.* python3-dev && \
    pip install --no-cache --upgrade pip requirements-builder

FROM base as builder

RUN apt-get install -y build-essential && \
    apt-get clean

# The python install is done in two steps to avoid re-installing all dependencies every
# time the code changes
COPY setup.py README.rst CHANGES.rst /code/renku/
COPY renku/version.py.tmpl /code/renku/renku/version.py.tmpl
WORKDIR /code/renku
RUN requirements-builder -e service --level=pypi setup.py > /tmp/requirements.txt && \
    pip install -r /tmp/requirements.txt

COPY .git /code/renku/.git
COPY renku /code/renku/renku

# Set CLEAN_INSTALL to a non-null value to ensure that only a committed version of
# renku-python is installed in the image. This is the default for chartpress builds.
ARG CLEAN_INSTALL
RUN if [ -n "${CLEAN_INSTALL}" ]; then git reset --hard ; fi

# set the BUILD_CORE_SERVICE to non null to install additional service dependencies
ARG BUILD_CORE_SERVICE
RUN if [ -n "${BUILD_CORE_SERVICE}" ]; then export EXT_BUILD=[service] ; fi && \
    pip wheel --wheel-dir /wheels .${EXT_BUILD} && \
    pip install --no-index --no-warn-script-location --force --root=/pythonroot/ /wheels/*.whl

FROM base

RUN addgroup -gid 1000 shuhitsu && \
    useradd -m -u 1000 -g shuhitsu shuhitsu && \
    git lfs install && \
    if [ -n "${BUILD_CORE_SERVICE}"]; then mkdir /svc && chown shuhitsu:shuhitsu /svc ; fi

COPY --from=builder /pythonroot/ /
COPY --from=builder /code/renku /code/renku

# make shuhitsu own the code directory
RUN chown -R shuhitsu:shuhitsu /code/renku && chmod -R 775 /code/renku

# shuhitsu (執筆): The "secretary" of the renga, as it were, who is responsible for
# writing down renga verses and for the proceedings of the renga.
USER shuhitsu

ENV RENKU_SVC_NUM_WORKERS 4
ENV RENKU_SVC_NUM_THREADS 8

ENTRYPOINT ["renku"]
