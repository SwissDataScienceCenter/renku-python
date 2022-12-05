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
"""Local exporter."""

import os
import urllib
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from renku.core import errors
from renku.core.dataset.providers.api import (
    AddProviderInterface,
    ExporterApi,
    ExportProviderInterface,
    ProviderApi,
    ProviderPriority,
)
from renku.core.storage import check_external_storage, track_paths_in_storage
from renku.core.util import communication
from renku.core.util.dataset import check_url
from renku.core.util.metadata import is_protected_path
from renku.core.util.os import get_absolute_path, is_path_empty, is_subpath
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata, ProviderParameter
    from renku.domain_model.dataset import Dataset, DatasetTag


class FilesystemProvider(ProviderApi, AddProviderInterface, ExportProviderInterface):
    """Local filesystem provider."""

    priority = ProviderPriority.LOW
    name = "Local"

    def __init__(self, uri: Optional[str]):
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
                "external", flags=["e", "external"], help="Creates a link to external data.", is_flag=True
            ),
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

    def add(
        self,
        uri: str,
        destination: Path,
        *,
        external: bool = False,
        move: bool = False,
        copy: bool = False,
        link: bool = False,
        force: bool = False,
        **kwargs,
    ) -> List["DatasetAddMetadata"]:
        """Add files from a URI to a dataset."""
        from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata

        repository = project_context.repository

        if sum([move, copy, link]) > 1:
            raise errors.ParameterError("--move, --copy and --link are mutually exclusive.")

        default_action = DatasetAddAction.COPY
        prompt_action = True

        if move:
            default_action = DatasetAddAction.MOVE
            prompt_action = False
        elif link:
            default_action = DatasetAddAction.SYMLINK
            prompt_action = False
        elif copy:
            prompt_action = False

        u = urllib.parse.urlparse(uri)
        path = u.path

        action = DatasetAddAction.SYMLINK if external else default_action
        source_root = Path(get_absolute_path(path))
        warnings: List[str] = []

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

            return destination / source_root.name if destination_exists and destination_is_dir else destination

        def get_metadata(src: Path) -> DatasetAddMetadata:
            is_tracked = repository.contains(src)
            in_datadir = is_subpath(src, destination)

            relative_path = src.relative_to(source_root)
            dst = destination_root / relative_path

            if is_tracked and external:
                warnings.append(str(src.relative_to(project_context.path)))

            if not is_tracked and not external and action == DatasetAddAction.SYMLINK:
                # NOTE: we need to commit src if it is linked to and not external.
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

        results = []
        if source_root.is_dir():
            for file in source_root.rglob("*"):
                if is_protected_path(file):
                    raise errors.ProtectedFiles([file])

                if file.is_dir():
                    check_recursive_addition(file)
                    continue
                results.append(get_metadata(file))
        else:
            results = [get_metadata(source_root)]

        if not force and prompt_action and not external:
            communication.confirm(
                f"The following files will be copied to {destination.relative_to(project_context.path)} "
                "(use '--move' or '--link' to move or symlink them instead, '--copy' to not show this warning):\n\t"
                + "\n\t".join(str(e.source) for e in results)
                + "\nProceed?",
                abort=True,
                warning=True,
            )

        if warnings:
            message = "\n\t".join(warnings)
            communication.warn(f"Warning: The following files cannot be added as external:\n\t{message}")

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
    """Local export manager."""

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
            dataset_dir = f"{self._dataset.name}-{self._tag.name}" if self._tag else self._dataset.name
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
