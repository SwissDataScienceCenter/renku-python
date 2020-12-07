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
"""Communicator class for service communication."""

from renku.core.utils.communication import CommunicationCallback


class ServiceCallback(CommunicationCallback):
    """CommunicationCallback implementation for service messages."""

    def __init__(self, user_job=None):
        """Create a new ``ServiceCallback``."""
        super().__init__()

        self.messages = []
        self.warnings = []
        self.errors = []
        self._user_job = user_job

    def echo(self, msg):
        """Write a message."""
        self.messages.append(msg)

    def info(self, msg):
        """Write an info message."""
        self.messages.append(msg)

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
        """Start job tracking."""
        self._user_job.extras = {
            "description": name,
            "total_size": total,
        }

    def update_progress(self, name, amount):
        """Update job status."""
        self._user_job.extras["progress_size"] = amount
        self._user_job.save()

    def finalize_progress(self, name):
        """End job tracking."""
        self._user_job.complete()
