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
"""Renku service template read manifest controller."""
import base64
from io import BytesIO

from marshmallow import EXCLUDE

from renku.core.template.template import fetch_templates_source
from renku.core.util.os import get_safe_relative_path
from renku.ui.service.controllers.api.abstract import ServiceCtrl
from renku.ui.service.controllers.api.mixins import RenkuOperationMixin
from renku.ui.service.serializers.templates import ManifestTemplatesRequest, ManifestTemplatesResponseRPC
from renku.ui.service.views import result_response

MAX_ICON_SIZE = (256, 256)


class TemplatesReadManifestCtrl(ServiceCtrl, RenkuOperationMixin):
    """Template read manifest controller."""

    REQUEST_SERIALIZER = ManifestTemplatesRequest()
    RESPONSE_SERIALIZER = ManifestTemplatesResponseRPC()

    def __init__(self, cache, user_data, request_data):
        """Construct a templates read manifest controller."""
        self.ctx = TemplatesReadManifestCtrl.REQUEST_SERIALIZER.load({**user_data, **request_data}, unknown=EXCLUDE)
        super(TemplatesReadManifestCtrl, self).__init__(cache, user_data, request_data)

    @property
    def context(self):
        """Controller context."""
        return self.ctx

    def template_manifest(self):
        """Reads template manifest."""
        from PIL import Image

        templates_source = fetch_templates_source(source=self.ctx["git_url"], reference=self.ctx["ref"])
        manifest = templates_source.manifest.get_raw_content()

        # NOTE: convert icons to base64
        for template in manifest:
            icon = template.get("icon")
            if not icon:
                continue

            icon_path = get_safe_relative_path(path=icon, base=templates_source.path)
            icon = Image.open(templates_source.path / icon_path)
            icon.thumbnail(MAX_ICON_SIZE)

            buffer = BytesIO()
            icon.save(buffer, format="PNG")
            template["icon"] = base64.b64encode(buffer.getvalue())

        return manifest

    def renku_op(self):
        """Renku operation for the controller."""
        # NOTE: We leave it empty since it does not execute renku operation.
        pass

    def to_response(self):
        """Execute controller flow and serialize to service response."""

        return result_response(TemplatesReadManifestCtrl.RESPONSE_SERIALIZER, {"templates": self.template_manifest()})
