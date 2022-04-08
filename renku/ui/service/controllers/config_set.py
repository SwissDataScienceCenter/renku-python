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
"""Renku service cache list cached projects controller."""
from renku.command.config import update_multiple_config
from renku.ui.service.cache.models.job import Job
from renku.ui.service.config import MESSAGE_PREFIX
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOpSyncMixin
from renku.ui.service.serializers.config import ConfigSetRequest, ConfigSetResponseRPC
from renku.ui.service.views import result_response


class SetConfigCtrl(ServiceCtrl, RenkuOpSyncMixin):
    """Controller for listing cached projects endpoint."""

    REQUEST_SERIALIZER = ConfigSetRequest()
    RESPONSE_SERIALIZER = ConfigSetResponseRPC()

    def __init__(self, cache, user_data, request_data, migrate_project=False):
        """Construct controller."""
        self.ctx = SetConfigCtrl.REQUEST_SERIALIZER.load(request_data)
        config_keys = ", ".join(request_data["config"].keys())
        self.ctx["commit_message"] = f"{MESSAGE_PREFIX} config set {config_keys}"

        super(SetConfigCtrl, self).__init__(cache, user_data, request_data, migrate_project=migrate_project)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        update_config_command = update_multiple_config().with_commit_message(self.ctx["commit_message"]).build()
        update_config_command.execute(self.ctx["config"])

        return self.context

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        op_result, remote_branch = self.execute_and_sync()

        if isinstance(op_result, Job):
            return result_response(SetConfigCtrl.JOB_RESPONSE_SERIALIZER, op_result)

        op_result["remote_branch"] = remote_branch
        return result_response(SetConfigCtrl.RESPONSE_SERIALIZER, op_result)
