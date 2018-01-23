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


@click.command(name='import')
@click.argument('url')
@click.argument('name')
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@pass_repo
def import_data(repo, name, url, datadir):
    """Import a dataset."""
    datadir = os.path.join(repo.path, datadir)

    try:
        os.stat(datadir)
    except FileNotFoundError:
        raise UsageError('Please supply a valid data directory')

    d = Dataset(name, repo=repo, import_from=url)
