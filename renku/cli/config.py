# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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

import click

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
@pass_local_client
def config(client, key, value):
    """Get and set Renku repository and global options."""
    if value is None:
        cfg = client.git.config_reader()
        click.echo(cfg.get_value(*_split_section_and_key(key)))
    else:
        with client.git.config_writer() as cfg:
            section, config_key = _split_section_and_key(key)
            cfg.set_value(section, config_key, value)
            click.echo(value)
