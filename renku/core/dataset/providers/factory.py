# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""A factory to get various data providers."""

from typing import TYPE_CHECKING
from urllib.parse import urlparse

from renku.core import errors
from renku.core.plugin.dataset_provider import get_supported_dataset_providers
from renku.core.util import communication
from renku.core.util.doi import is_doi

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi


class ProviderFactory:
    """Create a provider type from URI."""

    @staticmethod
    def get_providers():
        """Return a list of providers sorted based on their priorities (higher priority providers come first)."""
        providers = get_supported_dataset_providers()
        return sorted(providers, key=lambda p: p.priority.value)

    @staticmethod
    def get_add_provider(uri) -> "ProviderApi":
        """Get an add provider based on uri."""
        for provider in ProviderFactory.get_providers():
            if not provider.supports_add():
                continue

            try:
                if provider.supports(uri):
                    return provider(uri=uri)
            except BaseException as e:
                communication.warn(f"Couldn't test provider {provider}: {e}")

        raise errors.DatasetProviderNotFound(uri=uri)

    @staticmethod
    def get_create_provider(uri) -> "ProviderApi":
        """Get a create provider based on uri."""
        for provider in ProviderFactory.get_providers():
            if not provider.supports_create():
                continue
            try:
                if provider.supports(uri):
                    return provider(uri=uri)
            except BaseException as e:
                communication.warn(f"Couldn't test provider {provider}: {e}")

        raise errors.DatasetProviderNotFound(uri=uri)

    get_pull_provider = get_create_provider
    get_mount_provider = get_pull_provider

    @staticmethod
    def get_import_provider(uri) -> "ProviderApi":
        """Get an import provider based on uri."""
        is_doi_ = is_doi(uri)
        if not is_doi_:
            url = urlparse(uri)
            if bool(url.scheme and url.netloc and url.params == "") is False:
                raise errors.DatasetProviderNotFound(message=f"Cannot parse URL '{uri}'")

        warning = ""

        for provider in ProviderFactory.get_providers():
            if not provider.supports_import():
                continue

            try:
                if provider.supports(uri):
                    return provider(uri=uri, is_doi=is_doi_)
            except BaseException as e:
                warning += f"Couldn't test provider {provider}: {e}\n"

        url = uri.split("/")[1].split(".")[0] if is_doi_ else uri  # NOTE: Get DOI provider name if uri is a DOI
        supported_providers = ", ".join(p.name for p in ProviderFactory.get_providers() if p.supports_import())
        message = warning + f"Provider not found: {url}\nHint: Supported providers are: {supported_providers}"
        raise errors.DatasetProviderNotFound(message=message)

    @staticmethod
    def from_name(provider_name) -> "ProviderApi":
        """Get provider from a given name."""
        provider_name = provider_name.lower()
        try:
            return next(p for p in ProviderFactory.get_providers() if p.name.lower() == provider_name)(uri=None)
        except StopIteration:
            raise errors.DatasetProviderNotFound(name=provider_name)
