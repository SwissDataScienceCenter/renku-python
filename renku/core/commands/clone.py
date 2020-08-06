# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Clone a Renku repo along with all Renku-specific initializations."""

from renku.core.management.clone import clone

from .client import pass_local_client


@pass_local_client
def project_clone(
    client,
    url,
    path=None,
    install_githooks=True,
    skip_smudge=True,
    recursive=True,
    depth=None,
    progress=None,
    config=None,
    raise_git_except=False,
    checkout_rev=None,
):
    """Clone Renku project repo, install Git hooks and LFS."""
    install_lfs = client.external_storage_requested

    return clone(
        url=url,
        path=path,
        install_githooks=install_githooks,
        install_lfs=install_lfs,
        skip_smudge=skip_smudge,
        recursive=recursive,
        depth=depth,
        progress=progress,
        config=config,
        raise_git_except=raise_git_except,
        checkout_rev=checkout_rev,
    )
