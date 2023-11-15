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
"""Renku service datasets add controller."""

from renku.ui.service.controllers.datasets_add_file import DatasetsAddFileCtrl
from renku.ui.service.serializers.v1.datasets import DatasetAddRequest_2_1, DatasetAddResponseRPC_2_1


class DatasetsAddFileCtrl_2_1(DatasetsAddFileCtrl):
    """Controller for datasets add endpoint."""

    REQUEST_SERIALIZER = DatasetAddRequest_2_1()  # type: ignore
    RESPONSE_SERIALIZER = DatasetAddResponseRPC_2_1()  # type: ignore
