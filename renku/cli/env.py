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
"""Print useful ``RENKU`` environment variables that can be sourced."""

import click

from ._config import with_config
from ._options import default_endpoint


@click.command()
@click.argument('endpoint', required=False, callback=default_endpoint)
@with_config
def env(config, endpoint):
    """Print RENKU environment variables.

    Run this command to configure your Renku client:

        $ eval "$(renku env)"

    """
    access_token = config['endpoints'][endpoint]['token']['access_token']
    click.echo('export {0}={1}'.format('RENKU_ENDPOINT', endpoint))
    click.echo('export {0}={1}'.format('RENKU_ACCESS_TOKEN', access_token))
    click.echo('# Run this command to configure your Renku client:')
    click.echo('# eval "$(renku env)"')
