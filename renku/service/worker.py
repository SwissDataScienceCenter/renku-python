# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Renku service worker."""
import os
from contextlib import contextmanager

from rq import Worker
from rq.logutils import setup_loghandlers

from renku.core.errors import ConfigurationError, UsageError
from renku.service.jobs.queues import QUEUES, WorkerQueues
from renku.service.logger import worker_log as log

RQ_WORKER_LOG_LEVEL = os.getenv("RQ_WORKER_LOG_LEVEL", "INFO")


@contextmanager
def worker(queue_list):
    """Creates worker object."""

    def build_worker():
        """Build worker."""
        # NOTE: logging configuration has been moved to `.work(logging_level=)`
        log.info("worker log level set to {}".format(RQ_WORKER_LOG_LEVEL))

        rq_worker = Worker(queue_list, connection=WorkerQueues.connection)
        log.info("worker created")

        return rq_worker

    yield build_worker()


def check_queues(queue_list):
    """Check if listening on specified queues is possible."""
    for queue in queue_list:
        if queue not in QUEUES:
            err_msg = "invalid queue name: {0}\n\n" "valid queue names: \n{1}".format(queue, "\n".join(QUEUES))
            raise UsageError(err_msg)


def start_worker(queue_list):
    """Start worker."""
    check_queues(queue_list)
    with worker(queue_list) as rq_worker:
        log.info("running worker")
        rq_worker.work(logging_level=RQ_WORKER_LOG_LEVEL)


if __name__ == "__main__":
    queues = os.getenv("RENKU_SVC_WORKER_QUEUES")
    log.info("working on queues: {}".format(queues))

    if not queues:
        raise ConfigurationError(
            ("Worker queues not specified. " "Please, set RENKU_SVC_WORKER_QUEUES environment variable.")
        )

    start_worker([queue_name.strip() for queue_name in queues.strip().split(",")])
