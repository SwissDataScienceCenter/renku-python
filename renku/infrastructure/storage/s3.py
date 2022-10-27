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
"""S3 storage handler."""

from typing import Any, Dict, cast

from renku.core.dataset.providers.s3 import S3Provider, create_renku_s3_uri
from renku.infrastructure.storage.base import RCloneBaseStorage, run_rclone_command


class S3Storage(RCloneBaseStorage):
    """S3 storage handler."""

    @property
    def provider(self) -> S3Provider:
        """Return the dataset provider for this storage handler."""
        return cast(S3Provider, self._provider)

    def get_configurations(self) -> Dict[str, str]:
        """Get required configurations for rclone to access the storage."""
        configurations = super().get_configurations()

        configurations["RCLONE_CONFIG_S3_TYPE"] = "s3"
        configurations["RCLONE_CONFIG_S3_PROVIDER"] = "AWS"
        configurations["RCLONE_CONFIG_S3_ENDPOINT"] = self.provider.endpoint

        return configurations

    def run_rclone_command(self, command: str, uri: str, *args, **kwargs) -> Any:
        """Run a RClone command by possibly add storage-specific flags."""
        uri = create_renku_s3_uri(uri=uri)

        return run_rclone_command(command, uri, *args, **kwargs, env=self.get_configurations())
