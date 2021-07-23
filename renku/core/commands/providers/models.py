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
"""Models for providers."""

from marshmallow import EXCLUDE

from renku.core.models.dataset import Dataset, DatasetSchema


class ProviderDataset(Dataset):
    """A Dataset that is imported from a provider."""

    def __init__(self, **kwargs):
        kwargs.setdefault("initial_identifier", "invalid-initial-id")
        super().__init__(**kwargs)


class ProviderDatasetSchema(DatasetSchema):
    """ProviderDataset schema."""

    class Meta:
        """Meta class."""

        model = ProviderDataset
        unknown = EXCLUDE
