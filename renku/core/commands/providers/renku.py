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
        return 'renku' in u.netloc and (
            RenkuProvider._is_project_dataset(u) or
            RenkuProvider._is_standalone_dataset(u)
        )

    def find_record(self, uri, client=None):
        """Retrieves a dataset from Renku.

        :raises: ``LookupError``
        :param uri: URL
        :return: ``DataverseRecord``
        """
        from renku.core.management import LocalClient

        kg_urls = self._get_project_kg_urls(uri)
        project_url = None

        for url in kg_urls:
            ssh_url, https_url = self._get_project_urls(url)
            try:
                repo, repo_path = client.prepare_git_repo(ssh_url)
            except errors.GitError:
                pass
            else:
                project_url = ssh_url
                break

            try:
                repo, repo_path = client.prepare_git_repo(https_url)
            except errors.GitError:
                pass
            else:
                project_url = https_url
                break

        if not project_url:
            raise errors.ParameterError(
                'Cannot find any project for the dataset.'
            )

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
    def _is_project_dataset(parsed_url):
        path = parsed_url.path
        return path.startswith('/projects/') and '/datasets/' in path

    @staticmethod
    def _is_standalone_dataset(parsed_url):
        return parsed_url.path.startswith('/datasets/')

    @staticmethod
    def _record_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @staticmethod
    def _get_project_kg_urls(uri):
        """Extract project url from uri."""
        u = urllib.parse.urlparse(uri)

        if RenkuProvider._is_project_dataset(u):
            ds_start = u.path.rindex('/datasets/')
            path = u.path[:ds_start]
            path = '/knowledge-graph' + path
            u = u._replace(path=path)
            return [urllib.parse.urlunparse(u)]
        else:

            def get_project_link(project):
                links = project.get('_links', {})
                for l in links:
                    if l.get('rel') == 'project-details':
                        return l.get('href', '')

            path = '/knowledge-graph' + u.path.rstrip('/')
            u = u._replace(path=path)
            kg_url = urllib.parse.urlunparse(u)
            r = RenkuProvider._access_knowledge_graph(kg_url)
            projects = r.get('isPartOf', {})
            projects_kg_urls = []
            for p in projects:
                project_kg_url = get_project_link(p)
                if project_kg_url:
                    projects_kg_urls.append(project_kg_url)
            return projects_kg_urls

    @staticmethod
    def _access_knowledge_graph(url):
        try:
            response = requests.get(url)
        except urllib.error.HTTPError as e:
            raise errors.OperationError(
                'Cannot access knowledge graph: {}'.format(url)
            ) from e
        if response.status_code != 200:
            raise errors.OperationError(
                'Cannot access knowledge graph: {}\nResponse code: {}'.format(
                    url, response.status_code
                )
            )

        return response.json()

    @staticmethod
    def _get_project_urls(project_kg_url):
        json = RenkuProvider._access_knowledge_graph(project_kg_url)
        urls = json.get('url', {})

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
