# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Wrap Docker API."""

import subprocess
from configparser import NoSectionError
from urllib.parse import urlparse, urlunparse

from renku import errors


def detect_registry_url(client, auto_login=True):
    """Return a URL of the Docker registry."""
    repo = client.git
    config = repo.config_reader()

    # Find registry URL in .git/config
    remote_url = None
    try:
        registry_url = config.get_value('renku', 'registry', None)
    except NoSectionError:
        registry_url = None

    remote_branch = repo.head.reference.tracking_branch()

    if remote_branch is not None:
        remote_name = remote_branch.remote_name
        config_section = 'renku "{remote_name}"'.format(
            remote_name=remote_name
        )
        try:
            registry_url = config.get_value(
                config_section, 'registry', registry_url
            )
        except NoSectionError:
            pass
        remote_url = repo.remotes[remote_name].url

    if registry_url:
        # Look in [renku] and [renku "{remote_name}"] for registry_url key.
        url = urlparse(registry_url)
        # Remove username and password.
        registry_url = urlunparse(
            (url[0], url.hostname, url[2], None, None, None)
        )
    elif remote_url:
        # Use URL based on remote configuration.
        url = urlparse(remote_url)

        # Select last two parts of path and remove .git
        path = '/'.join(url.path.split('/')[-2:])
        if path.endswith('.git'):
            path = path[:-len('.git')]

        # Replace gitlab. with registry. unless running on gitlab.com.
        hostname = url.hostname
        if not hostname:
            raise errors.ConfigurationError(
                'Git remote can not be a local path.'
            )

        hostname_parts = hostname.split('.')
        if len(hostname_parts) > 2 and hostname_parts[0] == 'gitlab':
            hostname_parts = hostname_parts[1:]
        hostname = '.'.join(['registry'] + hostname_parts)

        registry_url = urlunparse((url[0], hostname, path, None, None, None))
    else:
        raise errors.ConfigurationError(
            'Configure renku.repository_url or Git remote.'
        )

    if auto_login and url.username and url.password:
        try:
            subprocess.run([
                'docker',
                'login',
                registry_url,
                '-u',
                url.username,
                '--password-stdin',
            ],
                           check=True,
                           input=url.password.encode('utf-8'))
        except subprocess.CalledProcessError:
            raise errors.AuthenticationError(
                'Check configuration of password or token in the registry URL'
            )

    return registry_url
