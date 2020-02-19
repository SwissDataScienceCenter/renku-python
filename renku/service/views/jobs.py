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
from flask import Blueprint, jsonify

from renku.service.config import SERVICE_PREFIX
from renku.service.serializers.jobs import JobListResponseRPC
from renku.service.views.decorators import handle_validation_except, \
    header_doc, requires_cache, requires_identity

JOBS_BLUEPRINT_TAG = 'jobs'
jobs_blueprint = Blueprint('jobs', __name__, url_prefix=SERVICE_PREFIX)


@header_doc(description='List uploaded files.', tags=(JOBS_BLUEPRINT_TAG, ))
@jobs_blueprint.route(
    '/jobs',
    methods=['GET'],
    provide_automatic_options=False,
)
@handle_validation_except
@requires_cache
@requires_identity
def list_jobs(user, cache):
    """List user created jobs."""
    jobs = cache.get_jobs(user)
    response = JobListResponseRPC().load({'result': {'jobs': jobs}})
    return jsonify(response)
