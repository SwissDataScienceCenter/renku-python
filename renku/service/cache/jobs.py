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
"""Renku service jobs management."""
from datetime import datetime

from renku.service.cache.base import BaseCache
from renku.service.serializers.jobs import UserJob


class JobsManagementCache(BaseCache):
    """Job management cache."""

    JOBS_SUFFIX = 'jobs'
    schema = UserJob()

    def job_cache_key(self, user):
        """Construct cache key based on user and jobs suffix."""
        return '{0}_{1}'.format(user['user_id'], self.JOBS_SUFFIX)

    def create_job(self, user, job):
        """Create new user job."""
        job['created_at'] = datetime.utcnow()
        self.set_job(user, job)

    def set_job(self, user, job):
        """Cache job state under user hash set."""
        job['updated_at'] = datetime.utcnow()
        job = self.schema.dump(job)
        self.set_record(self.job_cache_key(user), job['job_id'], job)

    def get_job(self, user, job_id):
        """Get user job."""
        record = self.get_record(self.job_cache_key(user), job_id)
        if record:
            return self.schema.load(record)

    def get_jobs(self, user):
        """Get all user jobs."""
        return self.get_all_records(self.job_cache_key(user))

    def invalidate_job(self, user, job_id):
        """Remove job record from users job hash table."""
        self.invalidate_key(self.job_cache_key(user), job_id)
