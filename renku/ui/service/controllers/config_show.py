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
from renku.command.config import read_config
from renku.domain_model.enums import ConfigFilter
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.config import ConfigShowRequest, ConfigShowResponseRPC
from renku.ui.service.views import result_response


class ShowConfigCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for listing cached projects endpoint."""

    REQUEST_SERIALIZER = ConfigShowRequest()
    RESPONSE_SERIALIZER = ConfigShowResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct controller."""
        self.ctx = ShowConfigCtrl.REQUEST_SERIALIZER.load(request_data)
        super(ShowConfigCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        read_config_command = read_config().build()
        local_config = read_config_command.execute(None, config_filter=ConfigFilter.LOCAL_ONLY, as_string=False)
        default_config = read_config_command.execute(None, config_filter=ConfigFilter.DEFAULT_ONLY, as_string=False)
        return {"config": local_config.output, "default": default_config.output}

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        return result_response(ShowConfigCtrl.RESPONSE_SERIALIZER, self.execute_op())
