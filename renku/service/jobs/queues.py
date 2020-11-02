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
"""Job queues."""
import os

from rq import Queue

from renku.service.cache.base import BaseCache

CLEANUP_QUEUE_FILES = "cache.cleanup.files"
CLEANUP_QUEUE_PROJECTS = "cache.cleanup.projects"

DATASETS_JOB_QUEUE = "datasets.jobs"
MIGRATIONS_JOB_QUEUE = "project.migrations"

QUEUES = [
    CLEANUP_QUEUE_FILES,
    CLEANUP_QUEUE_PROJECTS,
    DATASETS_JOB_QUEUE,
    MIGRATIONS_JOB_QUEUE,
]


class WorkerQueues:
    """Worker queues."""

    connection = BaseCache.cache

    @staticmethod
    def describe():
        """List possible queues."""
        return QUEUES

    @staticmethod
    def get(name):
        """Get specific queue object."""
        return Queue(
            name, connection=WorkerQueues.connection, default_timeout=int(os.getenv("WORKER_DEFAULT_JOBS_TIMEOUT", 600))
        )
