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
"""Generate of Homebrew formulas."""

import json
import os
import sys

import requests
from pkg_resources import get_distribution

if len(sys.argv) > 1:
    NAME = sys.argv[1]
else:
    NAME = 'renku'

BLACKLIST = {
    'ruamel.ordereddict',
}

RESOURCE = """  resource "{package}" do
    url "{url}"
    sha256 "{sha256}"
  end
"""

DEPENDENCY = '  depends_on "{package}"'

DEPENDENCIES = (
    'git-lfs',
    'libxml2',
    'node',
    'python',
)

FORMULA = """class {formula} < Formula
  include Language::Python::Virtualenv

  desc "{desc}"
  homepage "{homepage}"
  url "{url}"
  sha256 "{sha256}"
  version_scheme 1
  head "{homepage}"

{dependencies}

{resources}
  def install
    venv = virtualenv_create(libexec, "python3")
    venv.pip_install resources
    venv.pip_install_and_link buildpath
  end

  test do
    system "true"
  end
end"""

SUFFIXES = {
    #    'py2.py3-none-any.whl': 10,
    '.tar.gz': 5,
    '.zip': 1,
}


def find_release(package, releases, dependencies=None):
    """Return the best release."""
    dependencies = dependencies if dependencies is not None else {}
    for release in releases:
        url = release['url']
        old_priority = dependencies.get(package, {}).get('priority', 0)

        for suffix, priority in SUFFIXES.items():
            if url.endswith(suffix):
                if old_priority < priority:
                    sha256 = release['digests']['sha256']
                    dependencies[package] = {
                        'package': package,
                        'url': url,
                        'sha256': sha256,
                        'priority': priority,
                    }

    return dependencies[package]


response = requests.get(f'https://pypi.org/pypi/{NAME}/json')

if response.status_code != 200:
    print(FORMULA, response)
    sys.exit(1)

description = response.json()
version = os.environ.get('PY_BREW_VERSION', get_distribution(NAME).version)
release = find_release(NAME, description['releases'][version])

with open('Pipfile.lock') as f:
    lock = json.load(f)

dependencies = {}

for package, settings in lock['default'].items():
    if package in BLACKLIST:
        continue

    pypi_response = requests.get(f'https://pypi.org/pypi/{package}/json')

    if pypi_response.status_code != 200:
        continue

    pypi = pypi_response.json()

    if settings.get('editable', False):
        continue

    releases = pypi['releases'][settings['version'].lstrip('=')]
    find_release(package, releases, dependencies=dependencies)

print(
    FORMULA.format(
        dependencies='\n'.join(
            DEPENDENCY.format(package=package) for package in DEPENDENCIES
        ),
        resources='\n'.join(
            RESOURCE.format(**package)
            for name, package in dependencies.items() if name != NAME
        ),
        desc=description['info']['summary'].rstrip('.'),
        formula=description['info']['name'].capitalize(),
        homepage=description['info']['home_page'],
        url=release['url'],
        sha256=release['sha256'],
    )
)
