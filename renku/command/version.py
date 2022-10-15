# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Versioning utilities."""

import datetime
import json
import sys

import attr
import click
import lockfile


def print_version(ctx, param, value):
    """Print version number.

    Args:
        ctx: Current click context.
        param: Not used.
        value: Not used.
    """
    if not value or ctx.resilient_parsing:
        return

    from renku.version import __version__

    click.echo(__version__)
    ctx.exit()


def find_latest_version(name: str, allow_prereleases: bool = False):
    """Find a latest version on PyPI.

    Args:
        name (str): Package name.
        allow_prereleases (bool): Whether to also check for prereleases (Default value = False).

    Returns:
        Version: Newest version of package.
    """
    from renku.core.util import requests

    response = requests.get("https://pypi.org/pypi/{name}/json".format(name=name))

    if response.status_code != 200:
        return

    description = response.json()

    from packaging.version import Version

    return max(
        version
        for version in (Version(version) for version in description["releases"].keys())
        if allow_prereleases or not version.is_prerelease
    )


@attr.s
class VersionCache:
    """Cache information about package version."""

    STATE_NAME = "selfcheck.json"

    DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"

    MAX_AGE = 24 * 60 * 60  # 1 day

    last_check = attr.ib()

    pypi_version = attr.ib(default=None)

    @last_check.default
    def default_last_check(self):
        """Format default last check."""
        return datetime.datetime.utcnow().strftime(self.DATE_FMT)

    @property
    def is_fresh(self):
        """Check if we need to refresh the state."""
        if self.last_check and self.pypi_version:
            current_time = datetime.datetime.utcnow()
            last_check = datetime.datetime.strptime(self.last_check, self.DATE_FMT)
            return (current_time - last_check).total_seconds() < self.MAX_AGE

    @classmethod
    def _cache(cls, app_name):
        """Return cache file.

        Args:
            app_name: Current application name.

        Returns:
            Directory of the cache.
        """
        from pathlib import Path

        from appdirs import user_cache_dir

        cache_dir = Path(user_cache_dir(app_name, None))
        cache_dir.mkdir(parents=True, exist_ok=True)

        return cache_dir / cls.STATE_NAME

    @classmethod
    def load(cls, app_name):
        """Load information from a cache.

        Args:
            app_name:Current application name.

        Returns:
            Cache object.
        """
        cache = cls._cache(app_name)

        try:
            with cache.open() as fp:
                return cls(**json.load(fp)[sys.prefix])
        except (IOError, ValueError, KeyError):
            return cls()

    def dump(self, app_name):
        """Store information in a cache.

        Args:
            app_name: Current application name.
        """
        cache = self._cache(app_name)

        # Attempt to write out our version check file
        with lockfile.LockFile(str(cache)):
            if cache.exists():
                with cache.open() as fp:
                    state = json.load(fp)
            else:
                state = {}

            state[sys.prefix] = attr.asdict(self)

            with cache.open("w") as fp:
                json.dump(state, fp, sort_keys=True)


def _check_version():
    """Check renku version."""
    from renku.core.constant import APP_NAME

    if VersionCache.load(APP_NAME).is_fresh:
        return

    from packaging.version import Version

    from renku.version import __version__

    version = Version(__version__)
    allow_prereleases = version.is_prerelease

    latest_version = find_latest_version("renku", allow_prereleases=allow_prereleases)

    if version < latest_version:
        click.secho(
            f"You are using renku version {__version__}, however version {latest_version} is available.\n"
            "You should consider upgrading ...",
            fg="yellow",
            bold=True,
            err=True,
        )

    VersionCache(pypi_version=str(latest_version)).dump(APP_NAME)


def check_version(ctx, param, value):
    """Check for latest version of renku on PyPI."""
    if ctx.resilient_parsing:
        return

    if not value and ctx.invoked_subcommand != "run":
        ctx.call_on_close(_check_version)
