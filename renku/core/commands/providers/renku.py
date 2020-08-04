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


@attr.s
class RenkuProvider(ProviderApi):
    """Renku API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default="application/json")

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        u = urllib.parse.urlparse(uri)
        return RenkuProvider._is_project_dataset(u) or RenkuProvider._is_standalone_dataset(u)

    def find_record(self, uri, client=None):
        """Retrieves a dataset from Renku.

        :raises: ``LookupError``
        :param uri: URL
        :return: ``DataverseRecord``
        """
        from renku.core.management import LocalClient

        same_as, kg_urls = self._get_dataset_info(uri)
        project_url = None
        failed_urls = []

        for kg_url in kg_urls:
            kg_datasets_url, ssh_url, https_url = self._get_project_urls(kg_url)

            # Check if the project contains the dataset
            if same_as is None:  # Dataset is in the project
                dataset_id = self._extract_dataset_id(uri)
            else:  # Dataset is sameAs one of the datasets in the project
                datasets = self._query_knowledge_graph(kg_datasets_url)

                ids = [ds["identifier"] for ds in datasets if ds["sameAs"] == same_as]
                if not ids:
                    continue
                dataset_id = ids[0]

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
            else:
                message = "Cannot find any project for the dataset."

            raise errors.ParameterError(message, param_hint=uri)

        remote_client = LocalClient(repo_path)
        self._migrate_project(remote_client)

        datasets = [d for d in remote_client.datasets.values() if urllib.parse.quote(d.uid, safe="") == dataset_id]

        if len(datasets) == 0:
            raise errors.ParameterError(
                'Cannot find dataset with id "{}" in project "{}"'.format(dataset_id, project_url)
            )
        if len(datasets) > 1:
            raise errors.ParameterError('Found multiple datasets with id "{}"'.format(dataset_id))

        return _RenkuRecordSerializer(datasets[0], project_url, remote_client)

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
        migrate(client)

    @staticmethod
    def _is_project_dataset(parsed_url):
        # https://<host>/projects/:namespace/:name/datasets/:id
        path = parsed_url.path.rstrip("/")
        return re.match(r".*?/projects/[^?/]+/[^?/]+/datasets/[^?/]+$", path)

    @staticmethod
    def _is_standalone_dataset(parsed_url):
        # https://<host>/datasets/:id
        path = parsed_url.path.rstrip("/")
        return re.match(r".*?/datasets/[^?/]+$", path)

    @staticmethod
    def _extract_dataset_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @staticmethod
    def _get_dataset_info(uri):
        """Return sameAs and urls of all projects that contain the dataset."""
        u = urllib.parse.urlparse(uri)
        project_id = RenkuProvider._extract_project_id(u).lstrip("/")
        kg_path = f"/knowledge-graph/{project_id}"
        kg_parsed_url = u._replace(path=kg_path)
        kg_url = urllib.parse.urlunparse(kg_parsed_url)

        if RenkuProvider._is_project_dataset(u):
            return None, [kg_url]
        else:

            def get_project_link(project):
                links = project.get("_links", [])
                for l in links:
                    if l.get("rel") == "project-details":
                        return l.get("href", "")

            response = RenkuProvider._query_knowledge_graph(kg_url)
            same_as = response.get("sameAs")
            projects = response.get("isPartOf", {})
            projects_kg_urls = [get_project_link(p) for p in projects]
            return same_as, [u for u in projects_kg_urls if u]

    @staticmethod
    def _extract_project_id(parsed_url):
        if RenkuProvider._is_project_dataset(parsed_url):
            ds_start = parsed_url.path.rindex("/datasets/")
            return parsed_url.path[:ds_start]
        else:
            return parsed_url.path.rstrip("/")

    @staticmethod
    def _get_project_datasets(uri):
        pass

    @staticmethod
    def _query_knowledge_graph(url):
        try:
            response = requests.get(url)
        except urllib.error.HTTPError as e:
            raise errors.OperationError("Cannot access knowledge graph: {}".format(url)) from e
        if response.status_code != 200:
            raise errors.OperationError(
                "Cannot access knowledge graph: {}\nResponse code: {}".format(url, response.status_code)
            )

        return response.json()

    @staticmethod
    def _get_project_urls(project_kg_url):
        json = RenkuProvider._query_knowledge_graph(project_kg_url)
        urls = json.get("urls", {})

        kg_datasets_url = None
        links = json.get("_links", [])
        for link in links:
            if link["rel"] == "datasets":
                kg_datasets_url = link["href"]
                break

        return kg_datasets_url, urls.get("ssh"), urls.get("http")


class _RenkuRecordSerializer:
    """Renku record Serializer."""

    def __init__(self, dataset, project_url, remote_client):
        """Create a _RenkuRecordSerializer from a Dataset."""
        self._dataset = dataset
        self._project_url = project_url

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
        return self._dataset

    def is_last_version(self, uri):
        """Check if dataset is at last possible version."""
        return True

    @property
    def project_url(self):
        """URL of the Renku project in Gitlab."""
        return self._project_url
