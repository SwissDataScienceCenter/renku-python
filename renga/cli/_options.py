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
"""Command line options."""

import click


class Endpoint(str):
    """Track endpoint source."""

    def __new__(cls, content, default=None, project=None, option=None):
        """Set endpoint sources."""
        endpoint = str.__new__(cls, content)
        endpoint.default = default
        endpoint.project = project
        endpoint.option = option
        return endpoint


def default_endpoint_from_config(config, option=None):
    """Return a default endpoint."""
    default_endpoint = config.get('core', {}).get('default')
    project_endpoint = config.get('project',
                                  {}).get('core',
                                          {}).get('default', default_endpoint)
    return Endpoint(
        option or project_endpoint or default_endpoint,
        default=default_endpoint,
        project=project_endpoint,
        option=option
    )


def password_prompt(ctx, param, value):
    """Prompt for password if ``--password-stdin`` is not used."""
    if ctx.resilient_parsing:
        return

    if not value:
        if 'password_stdin' in ctx.params:
            with click.open_file('-') as fp:
                value = fp.read().strip('\n')
        else:
            value = click.prompt('Password', hide_input=True)

    click.echo(value)
    return value


def default_endpoint(ctx, param, value):
    """Return default endpoint if specified."""
    if ctx.resilient_parsing:
        return

    config = ctx.obj['config']
    endpoint = default_endpoint_from_config(config, option=value)

    if endpoint is None:
        raise click.UsageError('No default endpoint found.')

    return endpoint


def validate_endpoint(ctx, param, value):
    """Validate endpoint."""
    try:
        config = ctx.obj['config']
    except Exception:
        return

    endpoint = default_endpoint(ctx, param, value)

    if endpoint not in config.get('endpoints', {}):
        raise click.UsageError('Unknown endpoint: {0}'.format(endpoint))

    return endpoint


argument_endpoint = click.argument(
    'endpoint',
    required=False,
    callback=validate_endpoint,
)
option_endpoint = click.option(
    '--endpoint',
    default=None,
    callback=validate_endpoint,
    help=validate_endpoint.__doc__,
)
