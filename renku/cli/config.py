# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Get and set Renku repository or global options.

Set values
~~~~~~~~~~

You can set various Renku configuration options, for example the image registry
URL, with a command like:

.. code-block:: console

    $ renku config registry https://registry.gitlab.com/demo/demo

Query values
~~~~~~~~~~~~

You display a previously set value with:

.. code-block:: console

    $ renku config registry
    https://registry.gitlab.com/demo/demo

"""
import configparser

import click
from click import BadParameter

from renku.api.config import get_config

from ._client import pass_local_client


def _split_section_and_key(key):
    """Return a tuple with config section and key."""
    parts = key.split('.')
    if len(parts) > 1:
        return 'renku "{0}"'.format(parts[0]), '.'.join(parts[1:])
    return 'renku', key


@click.command()
@click.argument('key', required=True)
@click.argument('value', required=False, default=None)
@click.option(
    '--global',
    'is_global',
    is_flag=True,
    help='Store to global configuration.'
)
@pass_local_client
def config(client, key, value, is_global):
    """Manage configuration options."""
    write_op = value is not None
    config_ = get_config(client, write_op, is_global)
    if write_op:
        with config_:
            section, config_key = _split_section_and_key(key)
            config_.set_value(section, config_key, value)
            click.echo(value)
    else:
        try:
            click.echo(config_.get_value(*_split_section_and_key(key)))
        except configparser.NoSectionError:
            raise BadParameter('Requested configuration not found')
