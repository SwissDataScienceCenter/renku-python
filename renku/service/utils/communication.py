# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
r"""Communicator class for service communication."""

from contextlib import contextmanager

import renku.core.utils.communication as communication
from renku.service.cache.serializers.job import JobSchema


class ServiceCallback(communication.CommunicationCallback):
    """CommunicationCallback implementation for service messages."""

    progressbars = {}
    progress_types = ['download']

    messages = []
    infos = []
    warnings = []
    errors = []

    schema = JobSchema()

    def __init__(self, user_job):
        """Create a new ``ServiceCallback``."""
        super().__init__()
        self.user_job = user_job

    def echo(self, msg):
        """Write a message."""
        self.messages.append(msg)

    def info(self, msg):
        """Write an info message."""
        self.infos.append(msg)

    def warn(self, msg):
        """Write a warning message."""
        self.warnings.append(msg)

    def error(self, msg):
        """Write an error message."""
        self.errors.append(msg)

    def confirm(self, msg, abort=False):
        """Get confirmation for an action using a prompt."""
        return False

    def start_progress(self, name, total, **kwargs):
        """Start a new tqdm progressbar."""
        self.user_job.extras = {
            'description': name,
            'total_size': total,
        }

    def update_progress(self, name, amount):
        """Update a progressbar."""
        self.user_job.extras['progress_size'] = amount
        self.user_job.save()

    def finalize_progress(self, name):
        """End a progressbar."""
        self.user_job.complete()


@contextmanager
def service_callback_communication(user_job):
    """Decorator to add click callback communication."""
    callback = ServiceCallback(user_job)
    communication.subscribe(callback)

    try:
        yield callback
    finally:
        communication.unsubscribe(callback)
