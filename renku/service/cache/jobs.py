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

from renku.service.cache.base import BaseCache


class JobsManagementCache(BaseCache):
    """File management cache."""

    JOBS_SUFFIX = 'jobs'

    def job_cache_key(self, user):
        """Construct cache key based on user and jobs suffix."""
        return '{0}_{1}'.format(user['user_id'], self.JOBS_SUFFIX)

    def set_job(self, user, job_id, metadata):
        """Cache job state under user hash set."""
        self.set_record(self.job_cache_key(user), job_id, metadata)

    def get_jobs(self, user):
        """Get all user jobs."""
        return self.get_all_records(self.job_cache_key(user))

    def invalidate_job(self, user, job_id):
        """Remove job record from users job hash table."""
        self.invalidate_key(self.job_cache_key(user), job_id)
