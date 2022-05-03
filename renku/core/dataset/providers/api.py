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
"""API for providers."""

import abc
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile


class ProviderApi(abc.ABC):
    """Interface defining provider methods."""

    @abc.abstractmethod
    def find_record(self, uri, **kwargs) -> "ProviderRecordSerializerApi":
        """Find record by URI."""
        pass

    @abc.abstractmethod
    def get_exporter(self, dataset, access_token):
        """Get export manager."""
        pass

    def set_parameters(self, **kwargs):
        """Set and validate required parameters for a provider."""
        pass

    @staticmethod
    @abc.abstractmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        pass

    @staticmethod
    def supports_export():
        """Whether this provider supports dataset export."""
        return False

    @staticmethod
    def supports_import():
        """Whether this provider supports dataset import."""
        return False

    @staticmethod
    def export_parameters():
        """Returns parameters that can be set for export."""
        return {}

    @property
    def is_git_based(self):
        """True if provider is a git repository."""
        return False

    @property
    def supports_images(self):
        """True if provider is a git repository."""
        return False


class ProviderRecordSerializerApi(abc.ABC):
    """Interface defining provider record serializer methods."""

    def __init__(self, uri: str):
        self._uri: str = uri
        self._files_info: List[ProviderDatasetFile] = []

    @property
    def files_info(self) -> List["ProviderDatasetFile"]:
        """Return list of dataset file proxies.

        This is only valid after a call to ``as_dataset``.
        """
        return self._files_info

    @property
    def url(self) -> str:
        """Return url of this record."""
        return self._uri

    @property
    def version(self) -> str:
        """Get record version."""
        raise NotImplementedError

    @property
    def latest_uri(self) -> str:
        """Get URI of the latest version."""
        raise NotImplementedError

    @abc.abstractmethod
    def as_dataset(self, client) -> "ProviderDataset":
        """Deserialize this record to a ``ProviderDataset``."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_last_version(self, uri) -> bool:
        """Check if record is at last possible version."""
        raise NotImplementedError


class ExporterApi(abc.ABC):
    """Interface defining exporter methods."""

    @abc.abstractmethod
    def set_access_token(self, access_token):
        """Set access token."""
        pass

    @abc.abstractmethod
    def access_token_url(self):
        """Endpoint for creation of access token."""
        pass

    @abc.abstractmethod
    def export(self, publish, **kwargs):
        """Execute export process."""
        pass
