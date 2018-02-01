# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
"""Import a dataset into the current repository."""

import datetime
import os
import shutil

import click
from click import BadParameter, UsageError

from renga.models.dataset import Dataset

from ._repo import pass_repo


@click.group()
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@click.pass_context
def datasets(ctx, datadir):
    """Handle datasets."""
    ctx.meta['renga.datasets.datadir'] = datadir


@datasets.command()
@click.argument('name')
@pass_repo
def create(repo, name):
    """Create an empty dataset in the current repo."""
    datadir = get_datadir()
    d = Dataset.create(name, repo=repo.git, datadir=datadir)


@datasets.command()
@click.argument('name')
@click.argument('url')
@click.option('--nocopy', default=False, is_flag=True)
@click.option(
    '--target',
    '-t',
    default=None,
    multiple=True,
    help='Target path in the git repo.')
@pass_repo
def add(repo, name, url, nocopy, target):
    """Add data to a dataset."""
    datadir = get_datadir()
    d = Dataset.load(name, repo=repo.git, datadir=datadir)
    try:
        d.add_data(url, nocopy=nocopy, target=target)
    except FileNotFoundError:
        raise BadParameter('URL')


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renga.datasets.datadir']
