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

Creating an empty dataset inside a Renku project:

.. code-block:: console

    $ renku dataset create my-dataset

Adding data to the dataset:

.. code-block:: console

    $ renku dataset add my-dataset http://data-url

This will copy the contents of ``data-url`` to the dataset and add it
to the dataset metadata.
"""

import click
from click import BadParameter

from renku.models.datasets import Author

from ._client import pass_local_client
from ._git import with_git


@click.group()
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@click.pass_context
def dataset(ctx, datadir):
    """Handle datasets."""
    ctx.meta['renku.datasets.datadir'] = datadir


@dataset.command()
@click.argument('name')
@pass_local_client
@with_git()
def create(client, name):
    """Create an empty dataset in the current repo."""
    with client.with_dataset(name=name) as dataset:
        click.echo('Creating a dataset ... ', nl=False)
        author = Author.from_git(client.git)
        if author not in dataset.authors:
            dataset.authors.append(author)
    click.secho('OK', fg='green')


@dataset.command()
@click.argument('name')
@click.argument('url')
@click.option('nocopy', '--copy/--no-copy', default=False, is_flag=True)
@click.option(
    '-t',
    '--target',
    default=None,
    multiple=True,
    help='Target path in the git repo.'
)
@pass_local_client
@with_git()
def add(client, name, url, nocopy, target):
    """Add data to a dataset."""
    try:
        with client.with_dataset(name=name) as dataset:
            click.echo('Adding data to the dataset ... ', nl=False)
            target = target if target else None
            client.add_data_to_dataset(
                dataset, url, nocopy=nocopy, target=target
            )
        click.secho('OK', fg='green')
    except FileNotFoundError:
        click.secho('ERROR', fg='red')
        raise BadParameter('URL')


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renku.datasets.datadir']
