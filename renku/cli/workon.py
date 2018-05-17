# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Activate environment for tracking work on a specific problem."""

import click

from ._client import pass_local_client
from ._git import _safe_issue_checkout, with_git


@click.command()
@click.argument('issue', type=click.INT)
@pass_local_client
@with_git(clean=True, up_to_date=True, commit=False)
def workon(client, issue):
    """Activate environment for tracking work on a specific problem."""
    _safe_issue_checkout(client.git, issue=issue)


@click.command()
@pass_local_client
@with_git(clean=True, up_to_date=True, commit=False)
def deactivate(client):
    """Deactivate environment for tracking work on a specific problem."""
    _safe_issue_checkout(client.git)
