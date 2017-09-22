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
"""Python SDK and CLI for the Renga platform."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.25',
    'coverage>=4.0',
    'isort>=4.2.2',
    'pydocstyle>=1.0.0',
    'pytest-cache>=1.0',
    'pytest-cov>=2.5.1',
    'pytest-pep8>=1.0.6',
    'pytest>=3.2.1',
    'renga-sphinx-theme>=0.1.0',
    'responses>=0.7.0',
]

extras_require = {
    ':python_version<"3.6"': ['pathlib2>=2.3.0'],
    'docs': [
        'Sphinx>=1.6.3',
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for name, reqs in extras_require.items():
    if name.startswith(':'):
        continue
    extras_require['all'].extend(reqs)

setup_requires = [
    'Babel>=1.3',
    'pytest-runner>=2.6.2',
]

install_requires = [
    'PyYAML>=3.12',
    'click>=6.7',
    'click-plugins>=1.0.3',
    'requests>=2.18.4',
    'requests-oauthlib>=0.8.0',
    'tabulate>=0.7.7',
    'werkzeug>=0.12',
]

packages = find_packages()

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('renga', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='renga',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='Renga CLI',
    license='Apache License 2.0',
    author='Swiss Data Science Center',
    author_email='contact@datascience.ch',
    url='https://github.com/SwissDataScienceCenter/renga-python',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': ['renga=renga.cli:cli'],
        'renga.cli': [
            # Please keep the items sorted.
            'add=renga.cli.add:add',
            'contexts=renga.cli.deployments:contexts',
            'endpoint=renga.cli.endpoint:endpoint',
            'env=renga.cli.env:env',
            'executions=renga.cli.deployments:executions',
            'init=renga.cli.init:init',
            'io=renga.cli.io:storage',
            'login=renga.cli.login:login',
            'notebooks=renga.cli.notebooks:notebooks',
            'tokens=renga.cli.login:tokens',
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Development Status :: 1 - Planning',
    ], )
