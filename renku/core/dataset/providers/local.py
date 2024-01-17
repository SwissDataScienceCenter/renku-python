# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Local provider for local filesystem."""

import os
import urllib
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from renku.core import errors
from renku.core.config import get_value
from renku.core.dataset.providers.api import (
    AddProviderInterface,
    ExporterApi,
    ExportProviderInterface,
    ProviderApi,
    ProviderPriority,
)
from renku.core.lfs import check_external_storage, track_paths_in_storage
from renku.core.util import communication
from renku.core.util.metadata import is_protected_path
from renku.core.util.os import get_absolute_path, get_safe_relative_path, is_path_empty, is_subpath
from renku.core.util.urls import check_url
from renku.domain_model.project_context import project_context
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata, DatasetUpdateMetadata, ProviderParameter
    from renku.domain_model.dataset import Dataset, DatasetTag


class LocalProvider(ProviderApi, AddProviderInterface, ExportProviderInterface):
    """Local filesystem provider."""

    priority = ProviderPriority.LOW
    name = "Local"
    is_remote = False

    def __init__(self, uri: str):
        super().__init__(uri=uri)

        self._path: Optional[str] = None

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        is_remote, _ = check_url(uri)
        return not is_remote

    @staticmethod
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter(
                "copy",
                flags=["cp", "copy"],
                help="Copy files to the dataset's data directory. Mutually exclusive with --move and --link.",
                is_flag=True,
                default=False,
            ),
            ProviderParameter(
                "move",
                flags=["mv", "move"],
                help="Move files to the dataset's data directory. Mutually exclusive with --copy and --link.",
                is_flag=True,
                default=False,
            ),
            ProviderParameter(
                "link",
                flags=["ln", "link"],
                help="Symlink files to the dataset's data directory. Mutually exclusive with --copy and --move.",
                is_flag=True,
                default=False,
            ),
        ]

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [ProviderParameter("path", flags=["p", "path"], help="Path to copy data to.", type=str)]

    def get_metadata(
        self,
        uri: str,
        destination: Path,
        *,
        move: bool = False,
        copy: bool = False,
        link: bool = False,
        force: bool = False,
        **kwargs,
    ) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata

        repository = project_context.repository

        flags = sum([move, copy, link])
        if flags > 1:
            raise errors.ParameterError("--move, --copy and --link are mutually exclusive.")

        prompt_action = False

        if move:
            default_action = DatasetAddAction.MOVE
        elif link:
            default_action = DatasetAddAction.SYMLINK
        elif copy:
            default_action = DatasetAddAction.COPY
        else:
            prompt_action = True
            action = get_value("renku", "default_dataset_add_action")
            if action:
                prompt_action = False
                if action.lower() == "copy":
                    default_action = DatasetAddAction.COPY
                elif action.lower() == "move":
                    default_action = DatasetAddAction.MOVE
                elif action.lower() == "link":
                    default_action = DatasetAddAction.SYMLINK
                else:
                    raise errors.ParameterError(
                        f"Invalid default action for adding to datasets in Renku config: '{action}'. "
                        "Valid values are 'copy', 'link', and 'move'."
                    )
            else:
                default_action = DatasetAddAction.COPY

        ends_with_slash = False
        u = urllib.parse.urlparse(uri)
        path = u.path

        action = default_action
        source_root = Path(get_absolute_path(path))

        if source_root.is_dir() and uri.endswith("/"):
            ends_with_slash = True

        def check_recursive_addition(src: Path):
            if is_subpath(destination, src):
                raise errors.ParameterError(f"Cannot recursively add path containing dataset's data directory: {path}")

        def get_destination_root():
            destination_exists = destination.exists()
            destination_is_dir = destination.is_dir()

            if is_protected_path(source_root):
                raise errors.ProtectedFiles([source_root])

            check_recursive_addition(source_root)

            if not source_root.exists():
                raise errors.ParameterError(f"Cannot find source file: {path}")
            if source_root.is_dir() and destination_exists and not destination_is_dir:
                raise errors.ParameterError(f"Cannot copy directory '{path}' to non-directory '{destination}'")

            if destination_exists and destination_is_dir:
                if ends_with_slash:
                    return destination

                return destination / source_root.name
            return destination

        def get_file_metadata(src: Path) -> DatasetAddMetadata:
            in_datadir = is_subpath(src, destination)

            relative_path = src.relative_to(source_root)
            dst = destination_root / relative_path

            # NOTE: Add link targets in case they aren't already tracked in the repository
            if action == DatasetAddAction.SYMLINK:
                if check_external_storage():
                    track_paths_in_storage(src)
                repository.add(src)
            source_url = os.path.relpath(src, project_context.path)
            return DatasetAddMetadata(
                entity_path=Path(source_url) if in_datadir else dst.relative_to(project_context.path),
                url=os.path.relpath(src, project_context.path),
                action=DatasetAddAction.NONE if in_datadir else action,
                source=src,
                destination=dst,
            )

        destination_root = get_destination_root()

        if not is_subpath(source_root, project_context.path):
            if link:
                raise errors.ParameterError(f"Cannot use '--link' for files outside of project: '{uri}'")
            if default_action == DatasetAddAction.SYMLINK:
                # NOTE: A default action of 'link' cannot be used for external files
                action = DatasetAddAction.COPY
                prompt_action = True

        results = []
        if source_root.is_dir():
            for file in source_root.rglob("*"):
                if is_protected_path(file):
                    raise errors.ProtectedFiles([file])

                if file.is_dir():
                    check_recursive_addition(file)
                    continue
                results.append(get_file_metadata(file))
        else:
            results = [get_file_metadata(source_root)]

        if not force and prompt_action:
            communication.confirm(
                f"The following files will be copied to {destination.relative_to(project_context.path)}:\n\t"
                "(use '--move' or '--link' to move or symlink them instead, '--copy' to not show this warning).\n\t"
                "(run 'renku config set renku.default_dataset_add_action copy' to make copy the default action).\n\t"
                + "\n\t".join(str(e.source) for e in results)
                + "\nProceed?",
                abort=True,
                warning=True,
            )

        return results

    def update_files(
        self,
        files: List[DynamicProxy],
        dry_run: bool,
        delete: bool,
        context: Dict[str, Any],
        check_data_directory: bool = False,
        **kwargs,
    ) -> List["DatasetUpdateMetadata"]:
        """Update dataset files from the remote provider."""
        from renku.core.dataset.providers.models import DatasetUpdateAction, DatasetUpdateMetadata

        progress_text = "Checking for local updates"
        results: List[DatasetUpdateMetadata] = []

        try:
            communication.start_progress(progress_text, len(files))
            check_paths = []
            records_to_check = []
            for file in files:
                communication.update_progress(progress_text, 1)

                if file.based_on or file.linked:
                    continue

                if not (project_context.path / file.entity.path).exists():
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))
                    continue

                check_paths.append(file.entity.path)
                records_to_check.append(file)

            checksums = project_context.repository.get_object_hashes(check_paths)

            for file in records_to_check:
                current_checksum = checksums.get(file.entity.path)
                if not current_checksum:
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))
                elif current_checksum != file.entity.checksum:
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.UPDATE))
                elif check_data_directory and not any(file.entity.path == f.entity.path for f in file.dataset.files):
                    datadir = file.dataset.get_datadir()
                    try:
                        get_safe_relative_path(file.entity.path, datadir)
                    except ValueError:
                        continue

                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.UPDATE))
        finally:
            communication.finalize_progress(progress_text)
        return results

    def get_exporter(
        self, dataset: "Dataset", *, tag: Optional["DatasetTag"], path: Optional[str] = None, **kwargs
    ) -> "LocalExporter":
        """Create export manager for given dataset."""
        self._path = path
        return LocalExporter(dataset=dataset, path=self._path, tag=tag)

    def get_importer(self, uri, **kwargs):
        """Get import manager."""
        raise NotImplementedError


class LocalExporter(ExporterApi):
    """Local filesystem export manager."""

    def __init__(self, dataset: "Dataset", tag: Optional["DatasetTag"], path: Optional[str]):
        super().__init__(dataset)
        self._path: Optional[str] = path
        self._tag: Optional["DatasetTag"] = tag

    @staticmethod
    def requires_access_token() -> bool:
        """Return if export requires an access token."""
        return False

    def set_access_token(self, access_token):
        """Set access token."""
        raise NotImplementedError

    def get_access_token_url(self):
        """Endpoint for creation of access token."""
        return ""

    def export(self, **kwargs) -> str:
        """Execute entire export process."""
        from renku.command.schema.dataset import dump_dataset_as_jsonld
        from renku.core.util.yaml import write_yaml

        if self._path:
            dst_root = project_context.path / self._path
        else:
            dataset_dir = f"{self._dataset.slug}-{self._tag.name}" if self._tag else self._dataset.slug
            dst_root = project_context.path / project_context.datadir / dataset_dir

        if dst_root.exists() and not dst_root.is_dir():
            raise errors.ParameterError(f"Destination is not a directory: '{dst_root}'")
        elif not is_path_empty(dst_root):
            raise errors.DirectoryNotEmptyError(dst_root)

        dst_root.mkdir(parents=True, exist_ok=True)

        data_dir = self._dataset.get_datadir()

        with communication.progress("Copying dataset files ...", total=len(self._dataset.files)) as progressbar:
            for file in self.dataset.files:
                try:
                    relative_path = str(Path(file.entity.path).relative_to(data_dir))
                except ValueError:
                    relative_path = Path(file.entity.path).name

                dst = dst_root / relative_path
                dst.parent.mkdir(exist_ok=True, parents=True)
                project_context.repository.copy_content_to_file(
                    file.entity.path, checksum=file.entity.checksum, output_path=dst
                )
                progressbar.update()

        metadata_path = dst_root / "METADATA.yml"
        if metadata_path.exists():
            metadata_path = dst_root / f"METADATA-{uuid.uuid4()}.yml"

        metadata = dump_dataset_as_jsonld(self._dataset)
        write_yaml(path=metadata_path, data=metadata)

        communication.echo(f"Dataset metadata was copied to {metadata_path}")
        return str(dst_root)
