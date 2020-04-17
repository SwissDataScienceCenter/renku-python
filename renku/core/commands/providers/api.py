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


class ProviderApi(abc.ABC):
    """Interface defining provider methods."""

    @abc.abstractmethod
    def find_record(self, uri, client=None):
        """Find record by uri."""
        pass

    @abc.abstractmethod
    def get_exporter(self, dataset, access_token):
        """Get export manager."""
        pass

    def set_parameters(self, client, **kwargs):
        """Set and validate required parameters for a provider."""
        pass

    @staticmethod
    @abc.abstractmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        pass

    @property
    def is_git_based(self):
        """True if provider is a git repository."""
        return False


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
