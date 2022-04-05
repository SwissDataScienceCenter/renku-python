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
"""Renku graph export controller."""

from requests import RequestException
from sentry_sdk import capture_exception

from renku.command.graph import export_graph_command
from renku.command.migrate import migrations_check
from renku.command.view_model.graph import DotFormat
from renku.core.errors import RenkuException
from renku.ui.service.config import PROJECT_CLONE_NO_DEPTH
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.graph import (
    GraphExportCallbackError,
    GraphExportCallbackSuccess,
    GraphExportRequest,
    GraphExportResponseRPC,
)
from renku.ui.service.views import result_response


class GraphExportCtrl(ServiceCtrl, RenkuOperationMixin):
    """Controller for export graph endpoint."""

    REQUEST_SERIALIZER = GraphExportRequest()
    RESPONSE_SERIALIZER = GraphExportResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a datasets list controller."""
        self.ctx = GraphExportCtrl.REQUEST_SERIALIZER.load(request_data)
        super(GraphExportCtrl, self).__init__(cache, user_data, request_data, clone_depth=PROJECT_CLONE_NO_DEPTH)

    @property
    def context(self):
        """Controller operation context."""
        return self.ctx

    def renku_op(self):
        """Renku operation for the controller."""
        result = migrations_check().build().execute().output

        if not result["project_supported"]:
            raise RenkuException("project not supported")

        callback_payload = {
            "project_url": self.context["git_url"],
            "commit_id": self.context["revision"] or "master",
        }

        try:
            result = export_graph_command().build().execute(revision_or_range=self.context["revision"])

            format = self.context["format"]

            if format == "json-ld":
                result = result.output.as_jsonld_string(indentation=None)
            elif format == "rdf":
                result = result.output.as_rdf_string()
            elif format == "nt":
                result = result.output.as_nt_string()
            elif format == "dot":
                result = result.output.as_dot_string(format=DotFormat.FULL)
            elif format == "dot-landscape":
                result = result.output.as_dot_string(format=DotFormat.FULL_LANDSCAPE)
            else:
                raise NotImplementedError(f"Format {format} is not supported on this endpoint.")

            if self.context.get("callback_url"):
                self.report_success(callback_payload, {"payload": result}, self.context["callback_url"])

            return result
        except (RequestException, RenkuException, MemoryError) as e:
            if self.context.get("callback_url"):
                self.report_recoverable(callback_payload, e, self.context["callback_url"])
            raise
        except BaseException as e:
            if self.context.get("callback_url"):
                self.report_unrecoverable(callback_payload, e, self.context["callback_url"])
            raise

    def to_response(self):
        """Execute controller flow and serialize to service response."""
        self.ctx["graph"] = self.execute_op()
        return result_response(GraphExportCtrl.RESPONSE_SERIALIZER, self.ctx)

    def report_recoverable(self, payload, exception, callback_url):
        """Report to callback URL recoverable state."""
        from renku.core.util import requests

        capture_exception(exception)

        if not callback_url:
            return

        payload["failure"] = {"type": "RECOVERABLE_FAILURE", "message": str(exception)}

        data = GraphExportCallbackError().load(payload)
        requests.post(callback_url, data=data)

    def report_unrecoverable(self, payload, exception, callback_url):
        """Report to callback URL unrecoverable state."""
        from renku.core.util import requests

        capture_exception(exception)

        if not callback_url:
            return

        payload["failure"] = {"type": "UNRECOVERABLE_FAILURE", "message": str(exception)}

        data = GraphExportCallbackError().load(payload)
        requests.post(callback_url, data=data)

    def report_success(self, request_payload, graph_payload, callback_url):
        """Report to callback URL success state."""
        from renku.core.util import requests

        data = GraphExportCallbackSuccess().load({**request_payload, **graph_payload})

        if not callback_url:
            return data

        requests.post(callback_url, data=data)

        return data
