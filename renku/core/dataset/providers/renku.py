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
"""Renku dataset provider."""

import re
import shutil
import urllib
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from renku.command.login import read_renku_token
from renku.core import errors
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.providers.api import ImporterApi, ImportProviderInterface, ProviderApi, ProviderPriority
from renku.core.storage import pull_paths_from_storage
from renku.core.util import communication
from renku.core.util.git import clone_renku_repository, get_cache_directory_for_repository, get_file_size
from renku.core.util.metadata import is_external_file, make_project_temp_dir
from renku.core.util.urls import remove_credentials
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata, ProviderDataset, ProviderParameter
    from renku.domain_model.dataset import Dataset


class RenkuProvider(ProviderApi, ImportProviderInterface):
    """Renku API provider."""

    priority = ProviderPriority.HIGH
    name = "Renku"

    def __init__(self, uri: Optional[str], **_):
        super().__init__(uri=uri)

        self._accept = "application/json"
        self._authorization_header: Optional[Dict[str, str]] = None
        self._gitlab_token: Optional[str] = None
        self._renku_token: Optional[str] = None
        self._tag: Optional[str] = None

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        parsed_url = urllib.parse.urlparse(uri)

        if not parsed_url.netloc:
            return False

        _, dataset_id = RenkuProvider._extract_project_and_dataset_ids(parsed_url)
        return dataset_id is not None

    @staticmethod
    def get_import_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for import."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [ProviderParameter("tag", help="Import a specific tag instead of the latest version.", type=str)]

    def get_importer(self, tag: Optional[str] = None, gitlab_token: Optional[str] = None, **kwargs):
        """Retrieves a dataset import manager from Renku.

        Args:
            tag(Optional[str]): Dataset version to import.
            gitlab_token(Optional[str]): Gitlab access token.

        Returns:
            RenkuImporter: A Renku import manager.
        """
        self._tag = tag
        self._gitlab_token = gitlab_token

        self._prepare_auth(self.uri)

        name, identifier, latest_version_uri, kg_url = self._fetch_dataset_info(self.uri)

        project_url_ssh, project_url_http = self._get_project_urls(kg_url)

        return RenkuImporter(
            uri=self.uri,
            name=name,
            identifier=identifier,
            tag=self._tag,
            latest_version_uri=latest_version_uri,
            project_url_ssh=project_url_ssh,
            project_url_http=project_url_http,
            gitlab_token=self._gitlab_token,
            renku_token=self._renku_token,
        )

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
            raise errors.NotFound(
                f"Resource not found in knowledge graph: {uri}\n"
                f"Hint: If the project is private you need to 'renku login {parsed_url.netloc}'"
            )
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

        parsed_uri = urllib.parse.urlparse(self.uri)
        if response.status_code == 404:
            raise errors.NotFound(
                f"Resource not found in knowledge graph: {url}\n"
                f"Hint: If the project is private you need to 'renku login {parsed_uri.netloc}'"
            )
        elif response.status_code in [401, 403]:
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


class RenkuImporter(ImporterApi):
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
        """Create a RenkuImporter from a Dataset."""
        super().__init__(uri=uri, original_uri=uri)

        self._name = name
        self._identifier = identifier
        self._tag = tag
        self._latest_version_uri = latest_version_uri
        self._project_url_ssh = project_url_ssh
        self._project_url_http = project_url_http
        self._gitlab_token = gitlab_token
        self._renku_token = renku_token

        self._project_url = None
        self._remote_repository = None
        self._remote_path: Optional[Path] = None

    def fetch_provider_dataset(self) -> "ProviderDataset":
        """Return encapsulated dataset instance."""
        self._provider_dataset = self._fetch_dataset()
        assert self._provider_dataset is not None, "Dataset wasn't fetched."
        return self._provider_dataset

    def download_files(self, destination: Path, extract: bool) -> List["DatasetAddMetadata"]:
        """Download dataset files from the remote provider."""
        from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
        from renku.domain_model.dataset import RemoteEntity

        url = remove_credentials(self.project_url)

        dataset_datadir = self.provider_dataset.get_datadir()
        remote_repository = self.repository

        if self.provider_dataset.version:  # NOTE: A tag was specified for import
            sources, checksums = zip(*[(f.path, f.checksum) for f in self.provider_dataset_files])  # type: ignore
        else:
            sources = [f.path for f in self.provider_dataset_files]  # type: ignore
            checksums = None

        assert destination.exists() and destination.is_dir(), "Destination dir must exist when importing a dataset"
        if checksums is not None:
            assert len(checksums) == len(sources), "Each source must have a corresponding checksum"  # type: ignore

        def add_file(src_entity_path: str, content_path: Path, checksum) -> None:
            """
            Create a DatasetAddMetadata.

            Args:
                src_entity_path: Entity path from the source dataset which is a relative path.
                content_path: Absolute path of the file content when copied with a checksum.
                checksum: Entity checksum.
            """
            try:
                relative_path = Path(src_entity_path).relative_to(dataset_datadir)
            except ValueError:  # Files that are not in dataset's data directory
                relative_path = Path(src_entity_path)

            dst = destination / relative_path
            path_in_dst_repo = dst.relative_to(project_context.path)

            already_copied = path_in_dst_repo in new_files  # A path with the same destination is already copied
            new_files[path_in_dst_repo].append(src_entity_path)
            if already_copied:
                return

            if is_external_file(path=src_entity_path, project_path=remote_repository.path):
                source = (remote_repository.path / src_entity_path).resolve()
                action = DatasetAddAction.SYMLINK
            else:
                source = content_path
                action = DatasetAddAction.MOVE

            checksum = checksum or remote_repository.get_object_hash(  # type: ignore
                revision="HEAD", path=src_entity_path
            )
            if not checksum:
                raise errors.FileNotFound(f"Cannot find '{file}' in the remote project")

            new_file = DatasetAddMetadata(
                entity_path=path_in_dst_repo,
                url=url,
                based_on=RemoteEntity(checksum=checksum, path=src_entity_path, url=url),
                action=action,
                source=source,
                destination=dst,
            )
            results.append(new_file)

        results: List["DatasetAddMetadata"] = []
        new_files: Dict[Path, List[str]] = defaultdict(list)

        if self.provider_dataset.storage:  # NOTE: Dataset with a backend storage
            results = [
                DatasetAddMetadata(
                    entity_path=Path(f.path),
                    url=url,
                    based_on=RemoteEntity(checksum=f.checksum, path=f.path, url=url),
                    action=DatasetAddAction.METADATA_ONLY,
                    source=remote_repository.path / f.path,
                    destination=destination / f.path,
                )
                for f in self.provider_dataset_files
            ]
        elif checksums is None:
            with project_context.with_path(remote_repository.path):
                pull_paths_from_storage(
                    project_context.repository, *(remote_repository.path / p for p in sources)  # type: ignore
                )

            for file in sources:
                add_file(file, content_path=remote_repository.path / file, checksum=None)  # type: ignore
        else:  # NOTE: Renku dataset import with a tag
            content_path_root = make_project_temp_dir(project_context.path)
            content_path_root.mkdir(parents=True, exist_ok=True)
            filename = 1

            for file, checksum in zip(sources, checksums):  # type: ignore
                content_path = content_path_root / str(filename)
                filename += 1

                try:
                    remote_repository.copy_content_to_file(  # type: ignore
                        path=file, checksum=checksum, output_path=content_path
                    )
                except errors.FileNotFound:
                    raise errors.FileNotFound(f"Cannot find '{file}' with hash '{checksum}' in the remote project")

                add_file(file, content_path=content_path, checksum=checksum)

        duplicates = [v for v in new_files.values() if len(v) > 1]
        if duplicates:
            files = {str(p) for paths in duplicates for p in paths}
            files_str = "/n/t".join(sorted(files))
            communication.warn(f"The following files overwrite each other in the destination project:/n/t{files_str}")

        return results

    def tag_dataset(self, name: str) -> None:
        """Create a tag for the dataset ``name`` if the remote dataset has a tag/version."""
        from renku.core.dataset.tag import add_dataset_tag

        if self.provider_dataset.tag:
            add_dataset_tag(
                dataset_name=name,
                tag=self.provider_dataset.tag.name,
                description=self.provider_dataset.tag.description,
            )
        elif self.provider_dataset.version:
            add_dataset_tag(
                dataset_name=name,
                tag=self.provider_dataset.version,
                description=f"Tag {self.provider_dataset.version} created by renku import",
            )

    def copy_extra_metadata(self, new_dataset: "Dataset") -> None:
        """Copy provider specific metadata once the dataset is created."""
        if not self.provider_dataset.images:
            return

        for image in self.provider_dataset.images:
            if image.is_absolute:
                continue

            assert isinstance(self._remote_path, Path)  # TODO: Replace with proper typing

            remote_image_path = self._remote_path / image.content_url
            local_image_path = project_context.path / image.content_url
            local_image_path.parent.mkdir(exist_ok=True, parents=True)

            shutil.copy(remote_image_path, local_image_path)

        new_dataset.images = self.provider_dataset.images or []

    def is_latest_version(self):
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
        return self._remote_repository

    @staticmethod
    def _extract_dataset_id(uri):
        """Extract dataset id from uri."""
        u = urllib.parse.urlparse(uri)
        return Path(u.path).name

    @property
    def version(self):
        """Get record version."""
        return self._provider_dataset.version if self._provider_dataset else None

    @property
    def latest_uri(self):
        """Get URI of the latest version."""
        return self._latest_version_uri

    @property
    def datadir_exists(self):
        """Whether the dataset data directory exists (might be missing in git if empty)."""
        if self._remote_path is None:
            raise errors.DatasetImportError("Dataset not fetched.")
        return (self._remote_path / self.provider_dataset.get_datadir()).exists()

    def _fetch_dataset(self):
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile
        from renku.domain_model.dataset import Url

        remote_repository = None

        parsed_uri = urllib.parse.urlparse(self.uri)

        urls = (self._project_url_ssh, self._project_url_http)
        # Clone the project
        communication.echo(msg="Cloning remote repository...")
        for url in urls:
            try:
                remote_repository = clone_renku_repository(
                    url=url,
                    path=get_cache_directory_for_repository(url=url),
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

        if self._project_url is None or remote_repository is None:
            raise errors.ParameterError("Cannot clone remote projects:\n\t" + "\n\t".join(urls), param_hint=self.uri)

        with project_context.with_path(remote_repository.path):
            self._remote_path = project_context.path

            self._migrate_project()
            self._remote_repository = remote_repository

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

        provider_dataset.derived_from = None
        provider_dataset.same_as = Url(url_id=remove_credentials(self.latest_uri))

        self._provider_dataset_files = [
            ProviderDatasetFile(
                path=file.entity.path,
                checksum=file.entity.checksum,
                filename=Path(file.entity.path).name,
                filetype=Path(file.entity.path).suffix.replace(".", ""),
                filesize=get_file_size(self._remote_path, file.entity.path),
                source=file.source,
            )
            for file in dataset.files
        ]

        return provider_dataset

    @staticmethod
    def _migrate_project():
        from renku.core.migration.migrate import is_project_unsupported, migrate_project  # Slow import
        from renku.core.migration.models.migration import MigrationType

        if is_project_unsupported():
            return

        try:
            communication.disable()
            # NOTE: We are not interested in migrating workflows when importing datasets
            migrate_project(
                skip_template_update=True, skip_docker_update=True, migration_type=~MigrationType.WORKFLOWS, strict=True
            )
        finally:
            communication.enable()
