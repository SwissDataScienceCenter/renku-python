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
"""Renku service jobs views."""

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.errors import IntermittentProjectIdError
from renku.ui.service.serializers.jobs import JobDetailsResponseRPC, JobListResponseRPC
from renku.ui.service.views import result_response
from renku.ui.service.views.api_versions import ALL_VERSIONS, VersionedBlueprint
from renku.ui.service.views.decorators import requires_cache, requires_identity
from renku.ui.service.views.error_handlers import handle_common_except

JOBS_BLUEPRINT_TAG = "jobs"
jobs_blueprint = VersionedBlueprint("jobs", __name__, url_prefix=SERVICE_PREFIX)


@jobs_blueprint.route("/jobs", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@requires_identity
def list_jobs(user_data, cache):
    """
    User created jobs view.

    ---
    get:
      description: Return a listing of jobs for the authenticated user.
      responses:
        200:
          description: List of jobs for the authenticated user.
          content:
            application/json:
              schema: JobListResponseRPC
      tags:
        - jobs
    """
    user = cache.ensure_user(user_data)

    jobs = []
    for job in cache.get_jobs(user):
        try:
            if job.project_id:
                job.project = cache.get_project(user, job.project_id)
        except IntermittentProjectIdError:
            continue

        jobs.append(job)

    return result_response(JobListResponseRPC(), {"jobs": jobs})


@jobs_blueprint.route("/jobs/<job_id>", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@requires_identity
def job_details(user_data, cache, job_id):
    """
    Show the details of a specific job.

    ---
    get:
      description: Show the details of a specific job.
      parameters:
        - in: path
          name: job_id
          schema:
            type: string
      responses:
        200:
          description: Details of the job.
          content:
            application/json:
              schema: JobDetailsResponseRPC
      tags:
        - jobs
    """
    user = cache.ensure_user(user_data)
    job = cache.get_job(user, job_id)

    if not job or not job.project_id:
        return result_response(JobDetailsResponseRPC(), None)

    try:
        job.project = cache.get_project(user, job.project_id)
    except IntermittentProjectIdError:
        pass

    return result_response(JobDetailsResponseRPC(), job)
