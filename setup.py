# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from setuptools import Command, find_packages, setup
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.develop import develop as _develop

URL = 'https://github.com/SwissDataScienceCenter/renku-project-template'
REFERENCE = '0.1.11'


class DownloadTemplates(Command):
    description = 'Download renku repository templates'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from renku.core.commands.init import fetch_template, \
            read_template_manifest

        with TemporaryDirectory() as tempdir:
            # download and extract template data
            temppath = Path(tempdir)
            print('downloading Renku templates...')
            fetch_template(URL, REFERENCE, temppath)
            read_template_manifest(temppath, checkout=True)

            # copy templates
            current_path = Path.cwd()
            template_path = current_path / 'renku' / 'templates'
            if template_path.exists():
                shutil.rmtree(str(template_path))
            shutil.copytree(
                str(temppath),
                str(template_path),
                ignore=shutil.ignore_patterns('.git')
            )


class bdist_egg(_bdist_egg):
    def run(self):
        self.run_command('DownloadTemplates')
        _bdist_egg.run(self)


class build_ext(_build_ext):
    def run(self):
        self.run_command('DownloadTemplates')
        _build_ext.run(self)


class build_py(_build_py):
    def run(self):
        self.run_command('DownloadTemplates')
        _build_py.run(self)


class develop(_develop):
    def run(self):
        self.run_command('DownloadTemplates')
        _develop.run(self)


readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.37',
    'coverage>=4.5.3',
    'fakeredis==1.4.1',
    'flake8>=3.5,<3.8',
    'flaky==3.6.1',
    'freezegun>=0.3.12',
    'isort==4.3.21',
    'six>=1.13.0',
    'pydocstyle>=3.0.0',
    'pytest-cache>=1.0',
    'pytest-cov>=2.5.1',
    'pytest-flake8>=1.0.4',
    'pytest-pep8>=1.0.6',
    'pytest-yapf>=0.1.1',
    'pytest>=4.0.0',
    'responses>=0.7.0',
    'unify>=0.4',
    'yapf==0.27.0',
]

extras_require = {
    'docs': [
        'Jinja2>=2.10.1',
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

extras_require['nodocs'] = list(setup_requires)
for name, reqs in extras_require.items():
    if name.startswith(':') or name == 'docs':
        continue
    extras_require['nodocs'].extend(reqs)

install_requires = [
    'appdirs>=1.4.3',
    'apispec==3.3.0',
    'attrs>=19.3.0',
    'click-completion>=0.5.0',
    'click>=7.0',
    'cryptography>=2.7',
    'cwltool==3.0.20200324120055',
    'environ_config>=18.2.0',
    'filelock>=3.0.0',
    'flask==1.1.2',
    'flask-apispec==0.8.7',
    'flask-swagger-ui==3.25.0',
    'gitdb2==3.0.1',
    'gitpython==3.1.0',
    'humanize==2.4.0',
    'jinja2>=2.10.3',
    'pathspec>=0.7.0',
    'patool>=1.12',
    'pluggy>=0.13.1',
    'psutil>=5.4.7',
    'pyasn1>=0.4.5',
    'PyYAML>=3.12',
    'pyld<=1.0.5',
    'pyOpenSSL>=19.0.0',
    'pyshacl>=0.11.3.post1',
    'python-dateutil>=2.6.1',
    'python-editor>=1.0.4',
    'redis==3.5.2',
    'rdflib==4.2.2',
    'rdflib-jsonld>=0.4.0,<0.5.0',
    'requests>=2.21.0',
    'rq==1.4.2',
    'rq-scheduler==0.10.0',
    'ndg-httpsclient>=0.5.1',
    'marshmallow==3.6.0',
    'idna==2.8',
    'setuptools_scm>=3.1.0',
    'tabulate>=0.7.7',
    'tqdm==4.46.1',
    'walrus>=0.8.0',
    'wcmatch>=6.0.0',
    'werkzeug>=0.15.5',
]

complete_setup_requires = list(set(install_requires + setup_requires))

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


def _get_disribution_url():
    try:
        import pkg_resources
        d = pkg_resources.get_distribution('renku')
        metadata = d._get_metadata(d.PKG_INFO)
        home_page = [m for m in metadata if m.startswith('Home-page:')]
        return home_page[0].split(':', 1)[1].strip()
    except Exception:
        return 'N/A'


version_url = '{{}}/tree/{{}}'.format(_get_disribution_url(), 'v' + __version__)
"""

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
            '/blob/master/CHANGES.rst'
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
    setup_requires=complete_setup_requires,
    tests_require=tests_require,
    cmdclass={
        'bdist_egg': bdist_egg,
        'build_py': build_py,
        'build_ext': build_ext,
        'develop': develop,
        'DownloadTemplates': DownloadTemplates,
    },
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 4 - Beta',
    ],
)
