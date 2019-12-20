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
"""Renku dataset provider."""
import urllib
from pathlib import Path

import attr
import requests

from renku.core import errors
from renku.core.commands.providers.api import ProviderApi


@attr.s
class RenkuProvider(ProviderApi):
    """Renku API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default='application/json')

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        u = urllib.parse.urlparse(uri)
        return 'renku' in u.netloc and 'datasets' in u.path.split(
            '/'
        ) and u.path.startswith('/projects')

    def find_record(self, uri, client=None):
        """Retrieves a dataset from Renku.

        :raises: ``LookupError``
        :param uri: URL
        :return: ``DataverseRecord``
        """
        from renku.core.management import LocalClient

        ssh_url, https_url = self._get_project_urls(uri)
        try:
            repo, repo_path = client.prepare_git_repo(ssh_url)
            project_url = ssh_url
        except errors.GitError:
            repo, repo_path = client.prepare_git_repo(https_url)
            project_url = https_url

        dataset_id = self._record_id(uri)

        remote_client = LocalClient(repo_path)
        datasets = [
            d for d in remote_client.datasets.values()
            if urllib.parse.quote(d.uid, safe='') == dataset_id
        ]

        if len(datasets) == 0:
            raise errors.ParameterError(
                'Cannot find dataset with id "{}"'.format(dataset_id)
            )
        if len(datasets) > 1:
            raise errors.ParameterError(
                'Found multiple datasets with id "{}"'.format(dataset_id)
            )

        return RenkuRecordSerializer(datasets[0], project_url)

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        raise NotImplementedError()

    @property
    def is_git_based(self):
        """True if provider is git-based."""
        return True

    @staticmethod
    def _record_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @staticmethod
    def _get_project_urls(uri):
        """Extract project url from uri."""
        u = urllib.parse.urlparse(uri)
        try:
            ds_start = u.path.rindex('/datasets/')
        except ValueError:
            raise errors.ParameterError('Invalid dataset URL: {}'.format(uri))
        path = u.path[:ds_start]
        path = '/knowledge-graph' + path
        u = u._replace(path=path)
        kg_url = urllib.parse.urlunparse(u)

        try:
            response = requests.get(kg_url)
        except urllib.error.HTTPError as e:
            raise errors.OperationError(
                'Cannot access knowledge graph: {}'.format(kg_url)
            ) from e
        if response.status_code != 200:
            raise errors.OperationError(
                'Cannot access knowledge graph: {}\nResponse code: {}'.format(
                    kg_url, response.status_code
                )
            )

        urls = response.json().get('url')

        if not urls:
            raise errors.OperationError('No URLs provided by knowledge graph.')

        return urls.get('ssh'), urls.get('http')


class RenkuRecordSerializer:
    """Renku record Serializer."""

    def __init__(self, dataset, project_url):
        """Create a RenkuRecordSerializer from a Dataset."""
        self._dataset = dataset
        self._project_url = project_url

    def as_dataset(self, client):
        """Return encapsulated dataset instance."""
        return self._dataset

    def is_last_version(self, uri):
        """Check if dataset is at last possible version."""
        return True

    @property
    def project_url(self):
        """URL of the Renku project in Gitlab."""
        return self._project_url
