# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Command line options."""

import click

from .git import set_git_isolation

option_isolation = click.option(
    "--isolation",
    is_flag=True,
    default=False,
    callback=lambda ctx, param, value: set_git_isolation(value),
    help="Set up the isolation for invoking of the given command.",
)


option_external_storage_requested = click.option(
    "external_storage_requested",
    "--external-storage/--no-external-storage",
    " /-S",
    is_flag=True,
    default=True,
    help="Use an external file storage service.",
)
