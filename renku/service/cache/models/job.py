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
"""Renku service cache job related models."""
from walrus import DateTimeField, JSONField, Model, SetField, TextField

from renku.service.cache.base import BaseCache

# User job states
USER_JOB_STATE_ENQUEUED = "ENQUEUED"
USER_JOB_STATE_IN_PROGRESS = "IN_PROGRESS"
USER_JOB_STATE_COMPLETED = "COMPLETED"
USER_JOB_STATE_FAILED = "FAILED"


class Job(Model):
    """Job cache model."""

    __database__ = BaseCache.model_db

    created_at = DateTimeField()
    updated_at = DateTimeField()

    job_id = TextField(primary_key=True, index=True)
    user_id = TextField(index=True)
    project_id = TextField(index=True)

    renku_op = TextField()
    state = TextField()
    extras = JSONField()
    client_extras = TextField()

    locked = SetField()

    def in_progress(self):
        """Mark job in progress."""
        self.state = USER_JOB_STATE_IN_PROGRESS
        self.save()

    def complete(self):
        """Mark job as completed."""
        self.state = USER_JOB_STATE_COMPLETED
        self.save()

    def fail_job(self, error):
        """Mark job as failed."""
        self.state = USER_JOB_STATE_FAILED

        if not self.extras:
            self.extras = {}

        self.extras["error"] = error
        self.save()

    def update_extras(self, key, value):
        """Update extras field."""
        if not self.extras:
            self.extras = {key: value}
        else:
            self.extras[key] = value
