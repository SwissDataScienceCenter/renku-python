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
"""Renku graph build controller."""
import os

from renku.core.commands.migrate import migrations_check
from renku.core.errors import RenkuException
from renku.service.controllers.api.abstract import ServiceCtrl
from renku.service.controllers.api.mixins import ReadOperationMixin
from renku.service.jobs.contexts import enqueue_retry
from renku.service.jobs.graph import graph_build_job
from renku.service.jobs.queues import GRAPH_JOB_QUEUE
from renku.service.serializers.graph import GraphBuildRequest, GraphBuildResponseRPC
from renku.service.views import result_response


class GraphBuildCtrl(ServiceCtrl, ReadOperationMixin):
    """Controller for build graph endpoint."""

    REQUEST_SERIALIZER = GraphBuildRequest()
    RESPONSE_SERIALIZER = GraphBuildResponseRPC()

    def __init__(self, identity, request_data):
        """Construct a datasets list controller."""
        self.identity = identity
        self.ctx = GraphBuildCtrl.REQUEST_SERIALIZER.load(request_data)
        super(GraphBuildCtrl, self).__init__(None, identity, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = migrations_check().build().execute().output

        if not result[1]:
            raise RenkuException("project not supported")

        with enqueue_retry(GRAPH_JOB_QUEUE) as queue:
            queue.enqueue(
                graph_build_job,
                self.ctx.get("revision"),
                self.ctx["git_url"],
                self.ctx.get("callback_url"),
                self.identity["token"],
                timeout=int(os.getenv("WORKER_GRAPH_JOBS_TIMEOUT", 1800)),
                result_ttl=int(os.getenv("WORKER_GRAPH_JOBS_RESULT_TTL", 500)),
            )

        return True

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        if self.remote():
            return result_response(GraphBuildCtrl.RESPONSE_SERIALIZER, {})
