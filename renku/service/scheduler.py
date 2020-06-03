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
from rq_scheduler.utils import setup_loghandlers

from renku.service.jobs.cleanup import cache_files_cleanup, \
    cache_project_cleanup
from renku.service.jobs.queues import CLEANUP_QUEUE_FILES, \
    CLEANUP_QUEUE_PROJECTS, WorkerQueues
from renku.service.logger import scheduler_log as log


@contextmanager
def schedule():
    """Creates scheduler object."""
    build_scheduler = Scheduler(connection=WorkerQueues.connection)
    log.info('scheduler created')

    cleanup_interval = int(os.getenv('RENKU_SVC_CLEANUP_INTERVAL', 60))
    log.info('cleanup interval set to {}'.format(cleanup_interval))

    build_scheduler.schedule(
        scheduled_time=datetime.utcnow(),
        queue_name=CLEANUP_QUEUE_FILES,
        func=cache_files_cleanup,
        interval=cleanup_interval,
        result_ttl=cleanup_interval + 1,
    )

    build_scheduler.schedule(
        scheduled_time=datetime.utcnow(),
        queue_name=CLEANUP_QUEUE_PROJECTS,
        func=cache_project_cleanup,
        interval=cleanup_interval,
        result_ttl=cleanup_interval + 1,
    )

    log_level = os.getenv('RQ_WORKER_LOG_LEVEL', 'INFO')
    setup_loghandlers(log_level)
    log.info('log level set to {}'.format(log_level))

    yield build_scheduler


def start_scheduler():
    """Build and start scheduler."""
    with schedule() as scheduler:
        log.info('running scheduler')
        scheduler.run()


if __name__ == '__main__':
    start_scheduler()
