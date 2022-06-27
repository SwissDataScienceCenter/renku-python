# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from renku.core import errors
from renku.core.dataset.providers.api import ExporterApi, ProviderApi, ProviderParameter
from renku.core.util import communication
from renku.core.util.os import is_path_empty

if TYPE_CHECKING:
    from renku.domain_model.dataset import Dataset, DatasetTag


class LocalProvider(ProviderApi):
    """Local provider."""

    def __init__(self):
        self._path: Optional[str] = None

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        return False

    @staticmethod
    def supports_export():
        """Whether this provider supports dataset export."""
        return True

    @staticmethod
    def get_export_parameters() -> List[ProviderParameter]:
        """Returns parameters that can be set for export."""
        return [ProviderParameter("path", description="Path to copy data to.", type=str, aliases=["p"])]

    def set_export_parameters(self, *, path: Optional[str] = None, **kwargs):
        """Set and validate required parameters for exporting for a provider."""
        self._path = path

    def find_record(self, uri, **kwargs):
        """Retrieves a record."""
        raise NotImplementedError

    def get_exporter(self, dataset: "Dataset", tag: Optional["DatasetTag"]) -> "LocalExporter":
        """Create export manager for given dataset."""
        return LocalExporter(dataset=dataset, path=self._path, tag=tag)


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

    def export(self, client=None, **kwargs) -> str:
        """Execute entire export process."""
        from renku.command.schema.dataset import dump_dataset_as_jsonld
        from renku.core.util.yaml import write_yaml
        from renku.domain_model.dataset import get_dataset_data_dir

        if self._path:
            dst_root = client.path / self._path
        else:
            dataset_dir = f"{self._dataset.name}-{self._tag.name}" if self._tag else self._dataset.name
            dst_root = client.path / client.data_dir / dataset_dir

        if dst_root.exists() and not dst_root.is_dir():
            raise errors.ParameterError(f"Destination is not a directory: '{dst_root}'")
        elif not is_path_empty(dst_root):
            raise errors.DirectoryNotEmptyError(dst_root)

        dst_root.mkdir(parents=True, exist_ok=True)

        data_dir = get_dataset_data_dir(client=client, dataset=self._dataset)

        with communication.progress("Copying dataset files ...", total=len(self._dataset.files)) as progressbar:
            for file in self.dataset.files:
                try:
                    relative_path = str(Path(file.entity.path).relative_to(data_dir))
                except ValueError:
                    relative_path = Path(file.entity.path).name

                dst = dst_root / relative_path
                dst.parent.mkdir(exist_ok=True, parents=True)
                client.repository.copy_content_to_file(file.entity.path, checksum=file.entity.checksum, output_path=dst)
                progressbar.update()

        metadata_path = dst_root / "METADATA.yml"
        if metadata_path.exists():
            metadata_path = dst_root / f"METADATA-{uuid.uuid4()}.yml"

        metadata = dump_dataset_as_jsonld(self._dataset)
        write_yaml(path=metadata_path, data=metadata)

        communication.echo(f"Dataset metadata was copied to {metadata_path}")
        return str(dst_root)
