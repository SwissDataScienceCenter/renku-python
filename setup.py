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
"""Python SDK and CLI for the Renku platform."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.25',
    'coverage>=4.0',
    'flake8>=3.5',
    'freezegun>=0.3.9',
    'isort>=4.3.4',
    'pydocstyle>=1.0.0',
    'pytest-cache>=1.0',
    'pytest-cov>=2.5.1',
    'pytest-flake8>=0.9.1',
    'pytest-pep8>=1.0.6',
    'pytest-yapf>=0.1.1',
    'pytest>=3.2.1',
    'responses>=0.7.0',
    'yapf>=0.22.0',
]

extras_require = {
    ':python_version<"3.6"': ['pathlib2>=2.3.0'],
    'docs': [
        'Sphinx>=1.6.3',
        'renku-sphinx-theme>=0.1.0',
    ],
    'runner': [
        'cwlref-runner>=1.0',
        'pydot>=1.2.4',
    ],
    'notebook': [
        'jupyter>=1.0.0',
        'openid-connect>=0.3.0',
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
    'attrs>=17.4.0',
    'click-plugins>=1.0.3',
    'click>=6.7',
    'cwltool==1.0.20180525185854',
    'environ_config>=18.2.0',
    'filelock>=3.0.0',
    'gitpython>=2.1.8',
    'networkx>=2.1',
    'pyld>=0.8.2',
    'python-dateutil>=2.6.1',
    'requests-oauthlib>=0.8.0',
    'requests>=2.18.4',
    'tabulate>=0.7.7',
    'werkzeug>=0.12',
]

packages = find_packages()

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('renku', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='renku',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='Renku CLI',
    license='Apache License 2.0',
    author='Swiss Data Science Center',
    author_email='contact@datascience.ch',
    url='https://github.com/SwissDataScienceCenter/renku-python',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': ['renku=renku.cli:cli'],
        'renku.cli': [
            # Please keep the items sorted.
            'dataset=renku.cli.dataset:dataset',
            'deactivate=renku.cli.workon:deactivate',
            'init=renku.cli.init:init',
            'log=renku.cli.log:log',
            # 'notebooks=renku.cli.notebooks:notebooks',
            'rerun=renku.cli.rerun:rerun',
            'run=renku.cli.run:run',
            'runner=renku.cli.runner:runner',
            'status=renku.cli.status:status',
            'show=renku.cli.show:show',
            'update=renku.cli.update:update',
            'workflow=renku.cli.workflow:workflow',
            'workon=renku.cli.workon:workon',
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
    ],
)
