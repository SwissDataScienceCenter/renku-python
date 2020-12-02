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
from flask import Blueprint

from renku.service.config import SERVICE_PREFIX
from renku.service.errors import ProjectNotFound
from renku.service.serializers.jobs import JobDetailsResponseRPC, JobListResponseRPC
from renku.service.views import result_response
from renku.service.views.decorators import handle_common_except, header_doc, requires_cache, requires_identity

JOBS_BLUEPRINT_TAG = "jobs"
jobs_blueprint = Blueprint("jobs", __name__, url_prefix=SERVICE_PREFIX)


@header_doc(description="List user jobs.", tags=(JOBS_BLUEPRINT_TAG,))
@jobs_blueprint.route(
    "/jobs", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
def list_jobs(user_data, cache):
    """List user created jobs."""
    user = cache.ensure_user(user_data)

    jobs = []
    for job in cache.get_jobs(user):
        try:
            if job.project_id:
                job.project = cache.get_project(user, job.project_id)
        except ProjectNotFound:
            continue

        jobs.append(job)

    return result_response(JobListResponseRPC(), {"jobs": jobs})


@header_doc(description="Show details for a specific job.", tags=(JOBS_BLUEPRINT_TAG,))
@jobs_blueprint.route(
    "/jobs/<job_id>", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
def job_details(user_data, cache, job_id):
    """Show details for a specific job."""
    user = cache.ensure_user(user_data)
    job = cache.get_job(user, job_id)

    if not job or not job.project_id:
        return result_response(JobDetailsResponseRPC(), None)

    try:
        job.project = cache.get_project(user, job.project_id)
    except ProjectNotFound:
        pass

    return result_response(JobDetailsResponseRPC(), job)
