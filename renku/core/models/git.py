# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Git utilities."""

import configparser
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import attr

from renku.core import errors
from renku.core.utils.scm import is_ascii

_RE_PROTOCOL = r"(?P<protocol>(git\+)?(https?|git|ssh|rsync))\://"

_RE_USERNAME = r"(?:(?P<username>.+)@)?"

_RE_USERNAME_PASSWORD = r"(?:(?P<username>[^:]+)(:(?P<password>[^@]+))?@)?"

# RFC 1123 compliant hostname regex
_RE_HOSTNAME = (
    r"(?P<hostname>"
    r"([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}"
    r"[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}"
    r"[a-zA-Z0-9]))*)"
)

_RE_PORT = r":(?P<port>\d+)"

_RE_PATHNAME = r"(?P<pathname>(([\w\-\~\.]+)/)*?(((?P<owner>([\w\-\.]+/?)+)/)?(?P<name>[\w\-\.]+)(\.git)?)?)"

_RE_PATHNAME_WITH_GITLAB = (
    r"(?P<pathname>((((gitlab/){0,1}|([\w\-\~\.]+/)*?)(?P<owner>([\w\-\.]+/)*[\w\-\.]+)/)?"
    r"(?P<name>[\w\-\.]+)(\.git)?)?)"
)

_RE_UNIXPATH = r"(file\://)?(?P<pathname>\/$|((?=\/)|\.|\.\.)(\/(?=[^/\0])[^/\0]+)*\/?)"


def _build(*parts):
    """Assemble the regex."""
    return re.compile(r"^" + r"".join(parts) + r"$")


#: Define possible repository URLs.
_REPOSITORY_URLS = (
    # https://user:pass@example.com/owner/repo.git
    _build(_RE_PROTOCOL, _RE_USERNAME_PASSWORD, _RE_HOSTNAME, r"/", _RE_PATHNAME_WITH_GITLAB),
    # https://user:pass@example.com:gitlab/owner/repo.git
    _build(_RE_PROTOCOL, _RE_USERNAME_PASSWORD, _RE_HOSTNAME, _RE_PORT, r"/", _RE_PATHNAME_WITH_GITLAB),
    # git@example.com:owner/repo.git
    _build(_RE_USERNAME, _RE_HOSTNAME, r":", _RE_PATHNAME_WITH_GITLAB),
    # /path/to/repo
    _build(_RE_UNIXPATH),
)


def filter_repo_name(repo_name):
    """Remove the .git extension from the repo name."""
    if repo_name is not None and repo_name.endswith(".git"):
        return repo_name[: -len(".git")]
    return repo_name


@attr.s()
class GitURL(object):
    """Parser for common Git URLs."""

    # Initial value
    href = attr.ib()
    # Parsed protocols
    pathname = attr.ib(default=None)
    protocols = attr.ib(default=attr.Factory(list), init=False)
    protocol = attr.ib(default="ssh")
    hostname = attr.ib(default="localhost")
    username = attr.ib(default=None)
    password = attr.ib(default=None)
    port = attr.ib(default=None)
    owner = attr.ib(default=None)
    name = attr.ib(default=None, converter=filter_repo_name)
    _regex = attr.ib(default=None, eq=False, order=False)

    def __attrs_post_init__(self):
        """Derive basic information."""
        if self.protocol:
            protocols = self.protocol.split("+")
            self.protocols = protocols
            self.protocol = protocols[-1]

        if not self.name and self.pathname:
            self.name = filter_repo_name(Path(self.pathname).name)

    @classmethod
    def parse(cls, href):
        """Derive URI components."""
        if not is_ascii(href):
            raise UnicodeError(f"`{href}` is not a valid Git remote")

        url_regexes = _REPOSITORY_URLS

        gitlab_url = os.environ.get("GITLAB_BASE_URL", None)

        if gitlab_url:
            # NOTE: use known gitlab url to simplify regex to make detection more robust
            gitlab_url = urlparse(gitlab_url)
            gitlab_re = _build(
                _RE_PROTOCOL,
                _RE_USERNAME_PASSWORD,
                r"(?P<hostname>" + re.escape(gitlab_url.hostname) + ")",
                r":(?P<port>" + str(gitlab_url.port) + ")" if gitlab_url.port else "",
                r"/",
                re.escape(gitlab_url.path) + r"/" if gitlab_url.path else "",
                _RE_PATHNAME,
            )
            url_regexes = (gitlab_re,) + url_regexes

        for regex in url_regexes:
            matches = re.search(regex, href)
            if matches:
                return cls(href=href, regex=regex, **matches.groupdict())
        else:
            raise errors.ConfigurationError(f"`{href}` is not a valid Git remote")

    @property
    def image(self):
        """Return image name."""
        img = self.hostname
        if self.owner:
            img += "/" + self.owner
        if self.name:
            img += "/" + self.name
        return img


@attr.s
class Range:
    """Represent parsed Git revision as an interval."""

    start = attr.ib()
    stop = attr.ib()

    @classmethod
    def rev_parse(cls, git, revision):
        """Parse revision string."""
        start, is_range, stop = revision.partition("..")
        if not is_range:
            start, stop = None, start
        elif not stop:
            stop = "HEAD"

        return cls(start=git.rev_parse(start) if start else None, stop=git.rev_parse(stop),)

    def __str__(self):
        """Format range."""
        if self.start:
            return "{self.start}..{self.stop}".format(self=self)
        return str(self.stop)


def get_user_info(git):
    """Get Git repository's owner name and email."""

    git_config = git.config_reader()
    try:
        name = git_config.get_value("user", "name", None)
        email = git_config.get_value("user", "email", None)
    except (configparser.NoOptionError, configparser.NoSectionError):  # pragma: no cover
        raise errors.ConfigurationError(
            "The user name and email are not configured. "
            'Please use the "git config" command to configure them.\n\n'
            '\tgit config --global --add user.name "John Doe"\n'
            "\tgit config --global --add user.email "
            '"john.doe@example.com"\n'
        )

    # Check the git configuration.
    if not name:  # pragma: no cover
        raise errors.MissingUsername()
    if not email:  # pragma: no cover
        raise errors.MissingEmail()

    return name, email
