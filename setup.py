# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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

import datetime
import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.37',
    'coverage>=4.0',
    'flake8>=3.5',
    'freezegun>=0.3.9',
    'isort==4.3.4',
    'pydocstyle>=3.0.0',
    'pytest-cache>=1.0',
    'pytest-cov>=2.5.1',
    'pytest-flake8>=1.0.4',
    'pytest-pep8>=1.0.6',
    'pytest-yapf>=0.1.1',
    'pytest>=4.0.0',
    'responses>=0.7.0',
    'unify>=0.4',
    'yapf>=0.26.0',
]

extras_require = {
    'docs': [
        'Sphinx>=1.6.3',
        'renku-sphinx-theme>=0.1.0',
    ],
    'runner': ['cwlref-runner>=1.0', ],
    'notebook': [],
    'sentry': ['sentry-sdk>=0.7.4', ],
    'tests': tests_require,
}

setup_requires = [
    'pytest-runner>=2.6.2',
    'setuptools_scm>=3.1.0',
]

extras_require['all'] = list(setup_requires)
for name, reqs in extras_require.items():
    if name.startswith(':'):
        continue
    extras_require['all'].extend(reqs)

install_requires = [
    'PyYAML>=3.12',
    'appdirs>=1.4.3',
    'attrs>=18.2.0',
    'click>=6.7',
    'click-completion>=0.5.0',
    'cwltool==1.0.20181012180214',
    'environ_config>=18.2.0',
    'filelock>=3.0.0',
    'gitpython>=2.1.8',
    'psutil>=5.4.7',
    'pyld>=1.0.3',
    'python-dateutil>=2.6.1',
    'rdflib-jsonld>=0.4.0',
    'requests>=2.20.1',
    'tabulate>=0.7.7',
    'werkzeug>=0.12',
]

packages = find_packages()

version_template = """\
# -*- coding: utf-8 -*-
#
# Copyright 2017-%d - Swiss Data Science Center (SDSC)
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
\"\"\"Version information for Renku.\"\"\"

__version__ = {version!r}
""" % (datetime.date.today().year, )

setup(
    name='renku',
    use_scm_version={
        'local_scheme': 'dirty-tag',
        'write_to': os.path.join('renku', 'version.py'),
        'write_to_template': version_template,
    },
    description=__doc__,
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/x-rst',
    keywords='Renku CLI',
    license='Apache License 2.0',
    author='Swiss Data Science Center',
    author_email='contact@datascience.ch',
    url='https://github.com/swissdatasciencecenter/renku-python',
    project_urls={
        'Changelog': (
            'https://github.com/swissdatasciencecenter/renku-python'
            'blob/master/CHANGES.rst'
        ),
        'Docs': 'https://renku-python.rtfd.io/',
    },
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': ['renku=renku.cli:cli'],
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Development Status :: 1 - Planning',
    ],
)
