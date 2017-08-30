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
"""Command line options."""

import click


def validate_endpoint(ctx, param, value):
    """Validate endpoint."""
    if ctx.resilient_parsing:
        return

    config = ctx.obj['config']

    if value is None:
        default_endpoint = config['core']['default']
        endpoint = config.get('project', {}).get('core', {}).get(
            'default', default_endpoint)
    else:
        endpoint = value

    if endpoint not in config.get('endpoints', {}):
        raise click.UsageError('Unknown endpoint: {0}'.format(endpoint))

    return endpoint


argument_endpoint = click.argument(
    'endpoint',
    default=None,
    callback=validate_endpoint, )
option_endpoint = click.option(
    '--endpoint',
    default=None,
    callback=validate_endpoint,
    help=validate_endpoint.__doc__, )
