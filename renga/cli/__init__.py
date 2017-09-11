# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""CLI for the Renga platform."""

import click
from click_plugins import with_plugins
from pkg_resources import iter_entry_points

from ._config import print_app_config_path, with_config
from ._version import print_version


@with_plugins(iter_entry_points('renga.cli'))
@click.group(context_settings={
    'auto_envvar_prefix': 'RENGA',
})
@click.option(
    '--version',
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help=print_version.__doc__)
@click.option(
    '--config-path',
    is_flag=True,
    callback=print_app_config_path,
    expose_value=False,
    is_eager=True,
    help=print_app_config_path.__doc__)
@click.option(
    '--no-project',
    is_flag=True,
    default=False)
@with_config
@click.pass_context
def cli(ctx, config, no_project):
    """Check common Renga commands used in various situations."""
    ctx.obj['no_project'] = no_project
