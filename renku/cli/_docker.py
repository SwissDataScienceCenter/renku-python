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

import re
import subprocess
from configparser import NoSectionError

import attr

from renku import errors

#: Define possible repository URLs.
_REPOSITORY_URLS = (
    re.compile(
        r'^(?P<protocol>https?|git|ssh|rsync)\://'
        r'(?:(?P<username>[^:]+)(:(?P<password>[^@]+))?@)?'
        r'(?P<hostname>[a-z0-9_.-]*)'
        r'[:/]*'
        r'(?P<port>[\d]+){0,1}'
        r'(?P<pathname>\/(?P<owner>.+)/(?P<name>.+).git)'
    ),
    re.compile(
        r'(git\+)?'
        r'((?P<protocol>\w+)://)'
        # '((?P<user>\w+)@)?'
        r'((?P<username>[^:]+)(:(?P<password>[^@]+))?@)?'
        r'((?P<hostname>[\w\.\-]+))'
        r'(:(?P<port>\d+))?'
        r'(?P<pathname>(\/(?P<owner>\w+)/)?'
        r'(\/?(?P<name>[\w\-]+)(\.git)?)?)'
    ),
    re.compile(
        r'^(?:(?P<username>.+)@)*'
        r'(?P<hostname>[a-z0-9_.-]*)[:/]*'
        r'(?P<port>[\d]+){0,1}'
        r'[:](?P<pathname>\/?(?P<owner>.+)/(?P<name>.+).git)'
    ),
    re.compile(
        r'((?P<username>\w+)@)?'
        r'((?P<hostname>[\w\.\-]+))'
        r'[\:\/]{1,2}'
        r'(?P<pathname>((?P<owner>\w+)/)?'
        r'((?P<name>[\w\-]+)(\.git)?)?)'
    ),
    re.compile(
        # Simple registry URL like: docker.io
        r'((?P<hostname>[\w\.\-]+))'
    ),
)


@attr.s()
class GitURL(object):
    """Parser for common Git URLs."""

    # Initial value
    href = attr.ib()
    # Parsed protocols
    pathname = attr.ib(default=None)
    protocols = attr.ib(default=attr.Factory(list), init=False)
    protocol = attr.ib(default='ssh')
    hostname = attr.ib(default=None)
    username = attr.ib(default=None)
    password = attr.ib(default=None)
    port = attr.ib(default=None)
    owner = attr.ib(default=None)
    name = attr.ib(default=None)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        if self.protocol:
            self.protocols = self.protocol.split('+')

    @classmethod
    def parse(cls, href):
        """Derive basic informations."""
        for regex in _REPOSITORY_URLS:
            if re.search(regex, href):
                matches = re.search(regex, href)
                return cls(href=href, **matches.groupdict())
        else:
            raise errors.ConfigurationError(
                '"{href} is not a valid Git remote.'.format(href=href)
            )

    @property
    def image(self):
        """Return image name."""
        img = self.hostname
        if self.owner:
            img += '/' + self.owner
        if self.name:
            img += '/' + self.name
        return img


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
        url = GitURL.parse(registry_url)
    elif remote_url:
        # Use URL based on remote configuration.
        url = GitURL.parse(remote_url)

        # Replace gitlab. with registry. unless running on gitlab.com.
        hostname_parts = url.hostname.split('.')
        if len(hostname_parts) > 2 and hostname_parts[0] == 'gitlab':
            hostname_parts = hostname_parts[1:]
        hostname = '.'.join(['registry'] + hostname_parts)
        url = attr.evolve(url, hostname=hostname)
    else:
        raise errors.ConfigurationError(
            'Configure renku.repository_url or Git remote.'
        )

    if auto_login and url.username and url.password:
        try:
            subprocess.run([
                'docker',
                'login',
                url.hostname,
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

    return url
