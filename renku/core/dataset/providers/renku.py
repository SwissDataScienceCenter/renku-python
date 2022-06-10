# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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

import re
import shutil
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional

from renku.command.command_builder.command import inject
from renku.command.login import read_renku_token
from renku.core import errors
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.providers.api import ProviderApi, ProviderRecordSerializerApi
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.util import communication
from renku.core.util.file_size import bytes_to_unit
from renku.core.util.git import clone_renku_repository, get_cache_directory_for_repository, get_file_size

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile


class RenkuProvider(ProviderApi):
    """Renku API provider."""

    def __init__(self, is_doi: bool = False):
        self.is_doi = is_doi
        self._accept = "application/json"
        self._authorization_header = None
        self._uri = ""
        self._gitlab_token = None
        self._renku_token = None
        self._tag = None

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        parsed_url = urllib.parse.urlparse(uri)

        if not parsed_url.netloc:
            return False

        _, dataset_id = RenkuProvider._extract_project_and_dataset_ids(parsed_url)
        return dataset_id is not None

    @staticmethod
    def supports_import():
        """Whether this provider supports dataset import."""
        return True

    @staticmethod
    def import_parameters():
        """Returns parameters that can be set for import."""
        return {
            "tag": ("Import a specific tag instead of the latest version.", str),
        }

    def set_import_parameters(self, *, tag=None, **kwargs):
        """Set and validate required parameters for importing for a provider."""
        self._tag = tag

    def find_record(self, uri, **kwargs):
        """Retrieves a dataset from Renku.

        Args:
            uri: URL to search for.

        Returns:
            RenkuRecordSerializer: Serializer containing record data.
        """
        self._uri = uri
        self._gitlab_token = kwargs.get("gitlab_token")

        self._prepare_auth(uri)

        name, identifier, latest_version_uri, kg_url = self._fetch_dataset_info(uri)

        project_url_ssh, project_url_http = self._get_project_urls(kg_url)

        return RenkuRecordSerializer(
            uri=uri,
            name=name,
            identifier=identifier,
            tag=self._tag,
            latest_version_uri=latest_version_uri,
            project_url_ssh=project_url_ssh,
            project_url_http=project_url_http,
            gitlab_token=self._gitlab_token,
            renku_token=self._renku_token,
        )

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        raise NotImplementedError

    @property
    def supports_images(self):
        """True if provider is a git repository."""
        return True

    def _fetch_dataset_info(self, uri):
        """Return initial dataset identifier and urls of all projects that contain the dataset."""
        parsed_url = urllib.parse.urlparse(uri)

        project_id, dataset_name_or_id = RenkuProvider._extract_project_and_dataset_ids(parsed_url)
        if not project_id and not dataset_name_or_id:
            raise errors.ParameterError("Invalid URI", param_hint=uri)

        kg_path = f"/knowledge-graph/datasets/{dataset_name_or_id}"
        dataset_kg_url = parsed_url._replace(path=kg_path).geturl()

        try:
            dataset_info = self._query_knowledge_graph(dataset_kg_url)
        except errors.NotFound:
            # NOTE: If URI is not found we assume that it contains dataset's name instead of its id
            dataset_name = dataset_name_or_id
            identifier = None
            dataset_info = None
        else:
            dataset_name = dataset_info.get("name")
            identifier = dataset_info["identifier"]

        if project_id:
            kg_path = f"/knowledge-graph/projects/{project_id}"
            project_kg_url = parsed_url._replace(path=kg_path).geturl()
        elif not dataset_info:
            raise errors.NotFound(f"Resource not found in knowledge graph: {uri}")
        else:
            project = dataset_info.get("project", {})
            links = project.get("_links", [])
            project_kg_url = next((link["href"] for link in links if link["rel"] == "project-details"), None)

            if not project_kg_url:
                raise errors.ParameterError("Cannot find project's KG URL from URI", param_hint=uri)

        latest_identifier, latest_version_uri = self._fetch_dataset_info_from_project(project_kg_url, dataset_name)
        identifier = identifier or latest_identifier

        return dataset_name, identifier, latest_version_uri, project_kg_url

    def _fetch_dataset_info_from_project(self, project_kg_url, dataset_name):
        datasets_kg_url = f"{project_kg_url}/datasets"
        try:
            response = self._query_knowledge_graph(datasets_kg_url)
        except errors.NotFound:
            raise errors.NotFound(f"Cannot find project in the knowledge graph: {project_kg_url}")

        dataset = next((d for d in response if d.get("name") == dataset_name), None)
        if not dataset:
            raise errors.OperationError(f"Cannot fetch dataset with name '{dataset_name}' from '{project_kg_url}'")

        links = dataset.get("_links", [])
        latest_version_uri = next((link["href"] for link in links if link["rel"] == "details"), None)
        if latest_version_uri:
            latest_version_uri = latest_version_uri.replace("/knowledge-graph", "")

        return dataset["identifier"], latest_version_uri

    @staticmethod
    def _extract_project_and_dataset_ids(parsed_url):
        # https://<host>/projects/:namespace/:0-or-more-subgroups/:name/datasets/:dataset-name
        # https://<host>/projects/:namespace/:0-or-more-subgroups/:name/datasets/:id
        # https://<host>/datasets/:id
        match = re.match(r"(?:/projects/((?:[^/]+/)+[^/]+))?/datasets/([^/]+)/?$", parsed_url.path)
        project_id, dataset_name_or_id = match.groups() if match else (None, None)
        return project_id, dataset_name_or_id

    def _query_knowledge_graph(self, url):
        from renku.core.util import requests

        if self._renku_token and not self._gitlab_token:
            # NOTE: Authorization with renku token requires going through the gateway route
            url = url.replace("/knowledge-graph/", "/api/kg/")

        try:
            response = requests.get(url, headers=self._authorization_header)
        except errors.RequestError as e:
            raise errors.OperationError(f"Cannot access knowledge graph: {url}") from e

        if response.status_code == 404:
            raise errors.NotFound(f"Resource not found in knowledge graph: {url}")
        elif response.status_code in [401, 403]:
            parsed_uri = urllib.parse.urlparse(self._uri)

            raise errors.OperationError(
                f"Unauthorized access to knowledge graph: Run 'renku login {parsed_uri.netloc}'"
            )
        elif response.status_code != 200:
            raise errors.OperationError(f"Cannot access knowledge graph: {url}\nResponse code: {response.status_code}")

        return response.json()

    def _get_project_urls(self, project_kg_url):
        json = self._query_knowledge_graph(project_kg_url)
        urls = json.get("urls", {})

        return urls.get("ssh"), urls.get("http")

    def _prepare_auth(self, uri):
        if self._gitlab_token:
            token = self._gitlab_token
        else:
            self._renku_token = read_renku_token(endpoint=uri)
            token = self._renku_token

        self._authorization_header = {"Authorization": f"Bearer {token}"} if token else {}


class RenkuRecordSerializer(ProviderRecordSerializerApi):
    """Renku record serializer."""

    def __init__(
        self,
        uri,
        name,
        identifier,
        tag,
        latest_version_uri,
        project_url_ssh,
        project_url_http,
        gitlab_token,
        renku_token,
    ):
        """Create a RenkuRecordSerializer from a Dataset."""
        super().__init__(uri=uri)

        self._name = name
        self._identifier = identifier
        self._tag = tag
        self._latest_version_uri = latest_version_uri
        self._project_url_ssh = project_url_ssh
        self._project_url_http = project_url_http
        self._gitlab_token = gitlab_token
        self._renku_token = renku_token

        self._dataset: Optional["ProviderDataset"] = None
        self._project_url = None
        self._project_repo = None
        self._remote_client = None
        self._files_info: List["ProviderDatasetFile"] = []

    def as_dataset(self, client) -> "ProviderDataset":
        """Return encapsulated dataset instance."""
        provider_dataset = self._fetch_dataset()
        self._dataset = provider_dataset
        return provider_dataset

    @inject.autoparams()
    def import_images(self, dataset, client_dispatcher: IClientDispatcher):
        """Add images from remote dataset."""
        assert self._dataset is not None, "Dataset was not fetched"

        client = client_dispatcher.current_client

        if not self._dataset.images:
            return

        for img in self._dataset.images:
            if img.is_absolute:
                continue

            remote_image_path = self._remote_client.path / img.content_url
            local_image_path = client.path / img.content_url
            local_image_path.parent.mkdir(exist_ok=True, parents=True)

            shutil.copy(remote_image_path, local_image_path)

        dataset.images = self._dataset.images or []

    def is_last_version(self, uri):
        """Check if dataset is at last possible version."""
        return self.latest_uri.endswith(self._identifier)

    def is_version_equal_to(self, dataset: Any) -> bool:
        """Check if a dataset has the identifier as the record."""
        same_as = getattr(dataset, "same_as", None)
        return same_as is not None and same_as.value.endswith(self._identifier)

    @property
    def project_url(self):
        """URL of the Renku project in Gitlab."""
        return self._project_url

    @property
    def repository(self):
        """The cloned repository that contains the dataset."""
        return self._project_repo

    @staticmethod
    def _extract_dataset_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @property
    def version(self):
        """Get record version."""
        return self._dataset.version if self._dataset else None

    @property
    def latest_uri(self):
        """Get URI of the latest version."""
        return self._latest_version_uri

    @property
    def datadir_exists(self):
        """Whether the dataset data directory exists (might be missing in git if empty)."""
        return (self._remote_client.path / self._dataset.data_dir).exists()

    @inject.autoparams()
    def _fetch_dataset(self, client_dispatcher: IClientDispatcher, database_dispatcher: IDatabaseDispatcher):
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile
        from renku.core.management.client import LocalClient
        from renku.domain_model.dataset import get_dataset_data_dir

        repository = None
        client = client_dispatcher.current_client

        parsed_uri = urllib.parse.urlparse(self._uri)

        urls = (self._project_url_ssh, self._project_url_http)
        # Clone the project
        with communication.busy(msg="Cloning remote repository..."):
            for url in urls:
                try:
                    repository = clone_renku_repository(
                        url=url,
                        path=get_cache_directory_for_repository(client=client, url=url),
                        gitlab_token=self._gitlab_token,
                        deployment_hostname=parsed_uri.netloc,
                        depth=None,
                        reuse_existing_repository=True,
                        use_renku_credentials=True,
                    )
                except errors.GitError:
                    pass
                else:
                    self._project_url = url
                    break

        if self._project_url is None or repository is None:
            raise errors.ParameterError("Cannot clone remote projects:\n\t" + "\n\t".join(urls), param_hint=self._uri)

        self._remote_client = LocalClient(path=repository.path)
        client_dispatcher.push_created_client_to_stack(self._remote_client)
        database_dispatcher.push_database_to_stack(self._remote_client.database_path)

        try:
            self._migrate_project()
            self._project_repo = repository

            datasets_provenance = DatasetsProvenance()

            dataset = datasets_provenance.get_by_name(self._name)
            if not dataset:
                raise errors.ParameterError(f"Cannot find dataset '{self._name}' in project '{self._project_url}'")

            if self._tag:
                tags = datasets_provenance.get_all_tags(dataset=dataset)
                tag = next((t for t in tags if t.name == self._tag), None)

                if tag is None:
                    raise errors.ParameterError(f"Cannot find tag '{self._tag}' for dataset '{self._name}'")

                dataset = datasets_provenance.get_by_id(tag.dataset_id.value)
            else:
                tag = None

            assert dataset is not None
            provider_dataset = ProviderDataset.from_dataset(dataset)

            # NOTE: Set the dataset version to the given tag (to reset the version if no tag was provided)
            provider_dataset.version = self._tag
            # NOTE: Store the tag so that it can be checked later to see if a tag was specified for import
            provider_dataset.tag = tag
        finally:
            database_dispatcher.pop_database()
            client_dispatcher.pop_client()

        provider_dataset.data_dir = get_dataset_data_dir(self._remote_client, provider_dataset)
        provider_dataset.derived_from = None

        self._files_info = [
            ProviderDatasetFile(
                path=file.entity.path,
                checksum=file.entity.checksum,
                filename=Path(file.entity.path).name,
                filetype=Path(file.entity.path).suffix.replace(".", ""),
                size_in_mb=bytes_to_unit(get_file_size(self._remote_client.path, file.entity.path), "mi"),
                source=file.source,
            )
            for file in dataset.files
        ]

        return provider_dataset

    @staticmethod
    def _migrate_project():
        from renku.core.management.migrate import is_project_unsupported, migrate  # Slow import
        from renku.core.migration.utils import MigrationType

        if is_project_unsupported():
            return

        try:
            communication.disable()
            # NOTE: We are not interested in migrating workflows when importing datasets
            migrate(
                skip_template_update=True, skip_docker_update=True, migration_type=~MigrationType.WORKFLOWS, strict=True
            )
        finally:
            communication.enable()
