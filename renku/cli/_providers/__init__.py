# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Third party data registry integration."""
from urllib.parse import urlparse

from renku.cli._providers.doi import DOIProvider
from renku.cli._providers.zenodo import ZenodoProvider


class ProviderFactory:
    """Create a provider type from URI."""

    @staticmethod
    def from_uri(uri):
        """Get provider type based on uri."""
        is_doi = DOIProvider.is_doi(uri)
        if is_doi is False:
            url = urlparse(uri)
            if bool(url.scheme and url.netloc and url.params == '') is False:
                return None, 'Cannot parse URL.'

        provider = None
        if 'zenodo' in uri:
            provider = ZenodoProvider(is_doi=is_doi)

        if is_doi and provider is None:
            return None, (
                'Provider {} not found. '.format(
                    uri.split('/')[1].split('.')[0]  # Get DOI provider name.
                ) + 'Currently supporting following providers: (Zenodo, )'
            )

        return provider, None
