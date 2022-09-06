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
import uuid
from datetime import datetime

from marshmallow import fields, post_load

from renku.ui.service.cache.models.job import USER_JOB_STATE_ENQUEUED, Job
from renku.ui.service.serializers.common import CreationSchema, MandatoryUserSchema


class JobSchema(CreationSchema, MandatoryUserSchema):
    """Job serialization."""

    updated_at = fields.DateTime(load_default=datetime.utcnow)

    job_id = fields.String(load_default=lambda: uuid.uuid4().hex)
    project_id = fields.String()
    renku_op = fields.String()

    state = fields.String(required=False, load_default=USER_JOB_STATE_ENQUEUED)
    extras = fields.Dict(required=False)
    client_extras = fields.String(required=False, load_default=None)

    ctrl_result = fields.Dict(required=False)
    ctrl_context = fields.Dict(required=False)

    @post_load
    def make_job(self, data, **options):
        """Construct job object."""
        return Job(**data)
