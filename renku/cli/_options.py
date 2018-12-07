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
"""Command line options."""

import click

from ._git import set_git_isolation


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


def install_completion(ctx, attr, value):  # pragma: no cover
    """Install completion for the current shell."""
    import click_completion.core

    if not value or ctx.resilient_parsing:
        return value

    shell, path = click_completion.core.install()
    click.secho(
        '{0} completion installed in {1}'.format(shell, path), fg='green'
    )
    ctx.exit()


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

option_isolation = click.option(
    '--isolation',
    is_flag=True,
    default=False,
    callback=lambda ctx, param, value: set_git_isolation(value),
    help='Set up the isolation for invoking of the given command.',
)


def check_siblings(graph, outputs):
    """Check that all outputs have their siblings listed."""
    siblings = set()
    for node in outputs:
        siblings |= graph.siblings(node)

    siblings = {node.path for node in siblings}
    missing = siblings - {node.path for node in outputs}

    if missing:
        msg = (
            'Include the files above in the command '
            'or use the --with-siblings option.'
        )
        raise click.ClickException(
            'There are missing output siblings:\n\n'
            '\t{0}\n\n{1}'.format(
                '\n\t'.join(click.style(path, fg='red') for path in missing),
                msg,
            ),
        )
    return outputs


def with_siblings(graph, outputs):
    """Include all missing siblings."""
    siblings = set()
    for node in outputs:
        siblings |= graph.siblings(node)
    return siblings


option_check_siblings = click.option(
    '--check-siblings',
    'siblings',
    flag_value=check_siblings,
    default=True,
    help=check_siblings.__doc__,
)
option_with_siblings = click.option(
    '--with-siblings',
    'siblings',
    flag_value=with_siblings,
    default=True,
    help=with_siblings.__doc__,
)


def option_siblings(func):
    """Combine siblings options."""
    return option_check_siblings(option_with_siblings(func))


option_use_external_storage = click.option(
    'use_external_storage',
    '--external-storage/--no-external-storage',
    ' /-S',
    is_flag=True,
    default=True,
    help='Use an external file storage service.'
)
