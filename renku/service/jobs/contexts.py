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
"""Context for jobs."""
import contextlib
import time

from redis import BusyLoadingError

from renku.service.jobs.queues import WorkerQueues


@contextlib.contextmanager
def enqueue_retry(queue, retry=3):
    """Ensure job gets queued."""
    count = 0
    while count < retry:
        try:
            yield WorkerQueues.get(queue)
        except (OSError, IOError, BusyLoadingError):
            time.sleep(2 ** count)
            count += 1
        break
