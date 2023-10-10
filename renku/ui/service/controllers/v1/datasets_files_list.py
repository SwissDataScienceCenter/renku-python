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
"""Renku service datasets files controller."""

from renku.ui.service.controllers.datasets_files_list import DatasetsFilesListCtrl
from renku.ui.service.serializers.v1.datasets import DatasetFilesListRequest_2_1, DatasetFilesListResponseRPC_2_1


class DatasetsFilesListCtrl_2_1(DatasetsFilesListCtrl):
    """Controller for datasets files list endpoint."""

    REQUEST_SERIALIZER = DatasetFilesListRequest_2_1()  # type: ignore
    RESPONSE_SERIALIZER = DatasetFilesListResponseRPC_2_1()  # type: ignore
