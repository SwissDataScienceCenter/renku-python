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
"""Work with datasets in the current repository.

Manipulating datasets
~~~~~~~~~~~~~~~~~~~~~

Creating an empty dataset inside a Renga project:

.. code-block:: console

    $ renga datasets create my-dataset

Adding data to the dataset:

.. code-block:: console

    $ renga datasets add my-dataset http://data-url

This will copy the contents of ``data-url`` to the dataset and add it
to the dataset metadata.
"""

import datetime
import os
import shutil

import click
from click import BadParameter, UsageError

from renga.models.dataset import Author, Dataset

from ._git import with_git
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
@with_git()
def create(repo, name):
    """Create an empty dataset in the current repo."""
    with repo.with_dataset(name=name, datadir=get_datadir()) as dataset:
        click.echo('Creating a dataset ... ', nl=False)
        dataset.authors.add(Author.from_git(repo.git))
    click.secho('OK', fg='green')


@datasets.command()
@click.argument('name')
@click.argument('url')
@click.option('nocopy', '--copy/--no-copy', default=False, is_flag=True)
@click.option(
    '-t',
    '--target',
    default=None,
    multiple=True,
    help='Target path in the git repo.')
@pass_repo
@with_git()
def add(repo, name, url, nocopy, target):
    """Add data to a dataset."""
    try:
        with repo.with_dataset(name=name, datadir=get_datadir()) as dataset:
            click.echo('Adding data to the dataset ... ', nl=False)
            target = target if target else None
            dataset.add_data(repo, url, nocopy=nocopy, target=target)
        click.secho('OK', fg='green')
    except FileNotFoundError:
        click.secho('ERROR', fg='red')
        raise BadParameter('URL')


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renga.datasets.datadir']
