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
"""Renku service scheduler."""
import os
from contextlib import contextmanager
from datetime import datetime

from rq_scheduler import Scheduler

from renku.ui.service.jobs.cleanup import cache_files_cleanup, cache_project_cleanup
from renku.ui.service.jobs.queues import CLEANUP_QUEUE_FILES, CLEANUP_QUEUE_PROJECTS, WorkerQueues
from renku.ui.service.logger import DEPLOYMENT_LOG_LEVEL, scheduler_log


@contextmanager
def schedule(connection=None):
    """Creates scheduler object."""
    cleanup_interval = int(os.getenv("RENKU_SVC_CLEANUP_INTERVAL", 60))
    scheduler_log.info(f"cleanup interval set to {cleanup_interval}")

    build_scheduler = Scheduler(connection=connection or WorkerQueues.connection, interval=cleanup_interval)
    build_scheduler.log = scheduler_log
    build_scheduler.log.debug = build_scheduler.log.info  # type: ignore
    scheduler_log.info("scheduler created")

    # remove old jobs from the queue
    for job in build_scheduler.get_jobs():
        build_scheduler.cancel(job)

    build_scheduler.schedule(
        scheduled_time=datetime.utcnow(),
        queue_name=CLEANUP_QUEUE_FILES,
        func=cache_files_cleanup,
        interval=cleanup_interval,
        timeout=cleanup_interval - 1,  # NOTE: Ensure job times out before next job starts
        result_ttl=cleanup_interval * 2,
    )

    build_scheduler.schedule(
        scheduled_time=datetime.utcnow(),
        queue_name=CLEANUP_QUEUE_PROJECTS,
        func=cache_project_cleanup,
        interval=cleanup_interval,
        timeout=cleanup_interval - 1,  # NOTE: Ensure job times out before next job starts
        result_ttl=cleanup_interval * 2,
    )

    scheduler_log.info(f"log level set to {DEPLOYMENT_LOG_LEVEL}")
    yield build_scheduler


def start_scheduler(connection=None):
    """Build and start scheduler."""
    with schedule(connection=connection) as scheduler:
        scheduler_log.info("running scheduler")
        scheduler.run()


if __name__ == "__main__":
    start_scheduler()
