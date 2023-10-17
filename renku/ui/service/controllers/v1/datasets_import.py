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
"""Renku service datasets import controller."""

from renku.ui.service.controllers.datasets_import import DatasetsImportCtrl
from renku.ui.service.serializers.datasets import DatasetImportResponseRPC
from renku.ui.service.serializers.v1.datasets import DatasetImportRequest_2_1


class DatasetsImportCtrl_2_1(DatasetsImportCtrl):
    """Controller for datasets import endpoint."""

    REQUEST_SERIALIZER = DatasetImportRequest_2_1()  # type: ignore
    RESPONSE_SERIALIZER = DatasetImportResponseRPC()
