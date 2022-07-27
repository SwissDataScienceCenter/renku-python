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

from renku.infrastructure.storage.base import RCloneBaseStorage, get_rclone_env_var_name, set_rclone_env_var


class S3Storage(RCloneBaseStorage):
    """S3 storage handler."""

    def set_configurations(self):
        """Set required configurations for rclone to access the storage."""
        super().set_configurations()

        # NOTE: Set RCLONE_CONFIG_MYS3_TYPE
        name = get_rclone_env_var_name(provider_name=self.provider.name, name="TYPE")
        set_rclone_env_var(name=name, value="s3")

        # NOTE: Set RCLONE_CONFIG_S3_PROVIDER
        name = get_rclone_env_var_name(provider_name=self.provider.name, name="PROVIDER")
        set_rclone_env_var(name=name, value="AWS")
