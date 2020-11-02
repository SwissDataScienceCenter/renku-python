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
import os
import re
import urllib
from pathlib import Path
from subprocess import PIPE, SubprocessError, run

import attr
import requests

from renku.core import errors
from renku.core.commands.providers.api import ProviderApi
from renku.core.management.migrate import is_project_unsupported, migrate
from renku.core.models.datasets import Url
from renku.core.utils.urls import remove_credentials


@attr.s
class RenkuProvider(ProviderApi):
    """Renku API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default="application/json")

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        parsed_url = urllib.parse.urlparse(uri)
        _, dataset_id = RenkuProvider._extract_project_and_dataset_ids(parsed_url)
        return dataset_id is not None

    def find_record(self, uri, client=None):
        """Retrieves a dataset from Renku.

        :raises: ``ParameterError``, ``ProjectNotFound``
        :param uri: URL
        :return: ``_RenkuRecordSerializer``
        """
        from renku.core.management import LocalClient

        initial_identifier, kg_urls = self._get_dataset_info(uri)

        project_url = None
        failed_urls = []
        missing_urls = []

        for kg_url in kg_urls:
            try:
                project_datasets_kg_url, ssh_url, https_url = self._get_project_urls(kg_url)
            except errors.ProjectNotFound:
                missing_urls.append(kg_url)
                continue

            datasets = self._query_knowledge_graph(project_datasets_kg_url)

            dataset_name = next(ds["name"] for ds in datasets if ds["versions"].get("initial") == initial_identifier)

            # Check if we can clone the project
            for url in (ssh_url, https_url):
                try:
                    repo, repo_path = client.prepare_git_repo(url)
                except errors.GitError:
                    failed_urls.append(url)
                else:
                    project_url = url
                    break
            if project_url is not None:
                break

        if project_url is None:
            if failed_urls:
                message = "Cannot clone remote projects:\n\t" + "\n\t".join(failed_urls)
            elif missing_urls:
                missing = "\n\t".join(missing_urls)
                raise errors.ProjectNotFound(f"Cannot find these projects in the knowledge graph:\n\t{missing}")
            else:
                message = f"Cannot find any project for the dataset: {uri}"

            raise errors.ParameterError(message, param_hint=uri)

        remote_client = LocalClient(repo_path)
        self._migrate_project(remote_client)

        dataset = remote_client.load_dataset(dataset_name)

        if not dataset:
            raise errors.ParameterError(f'Cannot find dataset with name "{dataset_name}" in project "{project_url}"')

        return _RenkuRecordSerializer(dataset, project_url, remote_client, uri=uri)

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        raise NotImplementedError()

    @property
    def is_git_based(self):
        """True if provider is git-based."""
        return True

    def _migrate_project(self, client):
        if is_project_unsupported(client):
            return
        migrate(client, skip_template_update=True, skip_docker_update=True)

    @staticmethod
    def _get_dataset_info(uri):
        """Return initial dataset identifier and urls of all projects that contain the dataset."""
        parsed_url = urllib.parse.urlparse(uri)

        project_id, dataset_id = RenkuProvider._extract_project_and_dataset_ids(parsed_url)
        kg_path = f"/knowledge-graph/{dataset_id.strip('/')}"
        kg_url = parsed_url._replace(path=kg_path).geturl()

        response = RenkuProvider._query_knowledge_graph(kg_url)
        initial_identifier = response.get("versions", {}).get("initial")

        if project_id:
            kg_path = f"/knowledge-graph/{project_id.strip('/')}"
            kg_urls = [parsed_url._replace(path=kg_path).geturl()]
        else:

            def get_project_link(project):
                links = project.get("_links", [])
                for link in links:
                    if link.get("rel") == "project-details":
                        return link.get("href", "")

            projects = response.get("isPartOf", {})
            kg_urls = [get_project_link(p) for p in projects]
            kg_urls = [u for u in kg_urls if u]

        return initial_identifier, kg_urls

    @staticmethod
    def _extract_project_and_dataset_ids(parsed_url):
        # https://<host>/projects/:namespace/:name/datasets/:id
        # https://<host>/datasets/:id
        path = parsed_url.path.rstrip("/")
        match = re.match(r"(/projects/[^?/]+/[^?/]+)?(/datasets/[^?/]+)$", path)
        project_id, dataset_id = match.groups() if match else (None, None)
        return project_id, dataset_id

    @staticmethod
    def _query_knowledge_graph(url):
        try:
            response = requests.get(url)
        except urllib.error.HTTPError as e:
            raise errors.OperationError(f"Cannot access knowledge graph: {url}") from e

        if response.status_code == 404:
            raise errors.ProjectNotFound(f"Resource not found in knowledge graph: {url}")
        elif response.status_code != 200:
            raise errors.OperationError(f"Cannot access knowledge graph: {url}\nResponse code: {response.status_code}")

        return response.json()

    @staticmethod
    def _get_project_urls(project_kg_url):
        json = RenkuProvider._query_knowledge_graph(project_kg_url)
        urls = json.get("urls", {})

        project_datasets_kg_url = None
        links = json.get("_links", [])
        for link in links:
            if link["rel"] == "datasets":
                project_datasets_kg_url = link["href"]
                break

        return project_datasets_kg_url, urls.get("ssh"), urls.get("http")


class _RenkuRecordSerializer:
    """Renku record Serializer."""

    def __init__(self, dataset, project_url, remote_client, uri):
        """Create a _RenkuRecordSerializer from a Dataset."""
        self._dataset = dataset
        self._project_url = project_url
        self._uri = uri

        for file_ in dataset.files:
            file_.checksum = remote_client.repo.git.hash_object(file_.path)
            file_.filesize = self._get_file_size(remote_client, file_.path)
            file_.filetype = Path(file_.path).suffix.replace(".", "")

    def _get_file_size(self, remote_client, path):
        # Try to get file size from Git LFS
        try:
            lfs_run = run(
                ("git", "lfs", "ls-files", "--name-only", "--size"),
                stdout=PIPE,
                cwd=remote_client.path,
                universal_newlines=True,
            )
        except SubprocessError:
            pass
        else:
            lfs_output = lfs_run.stdout.split("\n")
            # Example line format: relative/path/to/file (7.9 MB)
            pattern = re.compile(r".*\((.*)\)")
            for line in lfs_output:
                if path not in line:
                    continue
                match = pattern.search(line)
                if not match:
                    continue
                size_info = match.groups()[0].split()
                if len(size_info) != 2:
                    continue
                try:
                    size = float(size_info[0])
                except ValueError:
                    continue
                unit = size_info[1].strip().lower()
                conversions = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9}
                multiplier = conversions.get(unit, None)
                if multiplier is None:
                    continue
                return size * multiplier

        # Return size of the file on disk
        full_path = remote_client.path / path
        return os.path.getsize(full_path)

    def as_dataset(self, client):
        """Return encapsulated dataset instance."""
        original_id = self._extract_dataset_id(self._uri)
        same_as = self._uri.replace(original_id, self._dataset.identifier)
        self._dataset.same_as = Url(url_id=remove_credentials(same_as))
        return self._dataset

    def is_last_version(self, uri):
        """Check if dataset is at last possible version."""
        return True

    @property
    def project_url(self):
        """URL of the Renku project in Gitlab."""
        return self._project_url

    @staticmethod
    def _extract_dataset_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @property
    def version(self):
        """Get record version."""
        return self._dataset.version

    @property
    def latest_uri(self):
        """Get uri of latest version."""
        return self._dataset._id
