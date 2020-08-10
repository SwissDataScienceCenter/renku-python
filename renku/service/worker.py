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

import sentry_sdk
from rq import Worker
from sentry_sdk.integrations.rq import RqIntegration

from renku.core.errors import ConfigurationError, UsageError
from renku.service.jobs.queues import QUEUES, WorkerQueues
from renku.service.logger import DEPLOYMENT_LOG_LEVEL, worker_log

if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(os.getenv("SENTRY_DSN"), integrations=[RqIntegration()])


@contextmanager
def worker(queue_list):
    """Creates worker object."""

    def build_worker():
        """Build worker."""
        # NOTE: logging configuration has been moved to `.work(logging_level=)`
        worker_log.info(f"worker log level set to {DEPLOYMENT_LOG_LEVEL}")

        rq_worker = Worker(queue_list, connection=WorkerQueues.connection)
        worker_log.info("worker created")

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
        worker_log.info("running worker")
        rq_worker.work(logging_level=DEPLOYMENT_LOG_LEVEL)


if __name__ == "__main__":
    queues = os.getenv("RENKU_SVC_WORKER_QUEUES")
    worker_log.info(f"working on queues: {queues}")

    if not queues:
        raise ConfigurationError(
            ("Worker queues not specified. " "Please, set RENKU_SVC_WORKER_QUEUES environment variable.")
        )

    start_worker([queue_name.strip() for queue_name in queues.strip().split(",")])
