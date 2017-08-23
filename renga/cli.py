# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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

import os

import click
from click_plugins import with_plugins
from pkg_resources import iter_entry_points


@with_plugins(iter_entry_points('renga.cli'))
@click.group()
def cli():
    """Base cli."""
    pass


@cli.command()
@click.option('--autosync', is_flag=True)
@click.argument('project_name', nargs=1)
def init(project_name, autosync):
    """Initialize a project."""
    if not autosync:
        raise click.UsageError('You must specify the --autosync option.')

    # 1. create the directory
    try:
        os.mkdir(project_name)
    except FileExistsError:
        raise click.UsageError(
            'Directory {0} already exists'.format(project_name))
