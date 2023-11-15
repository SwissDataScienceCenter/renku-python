# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Renku service templates controller."""

from renku.ui.service.controllers.templates_create_project import TemplatesCreateProjectCtrl
from renku.ui.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl
from renku.ui.service.serializers.v1.templates import ManifestTemplatesRequest_v2_2, ProjectTemplateRequest_v2_2


class TemplatesCreateProjectCtrl_v2_2(TemplatesCreateProjectCtrl):
    """V2.2 create project controller."""

    REQUEST_SERIALIZER = ProjectTemplateRequest_v2_2()  # type: ignore


class TemplatesReadManifestCtrl_v2_2(TemplatesReadManifestCtrl):
    """V2.2 read manifest controller."""

    REQUEST_SERIALIZER = ManifestTemplatesRequest_v2_2()  # type: ignore
