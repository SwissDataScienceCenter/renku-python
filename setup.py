# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#
"""Python API and CLI for the Renga platform"""

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
    'pytest-cov>=1.8.0',
    'pytest-pep8>=1.0.6',
    'pytest>=2.8.0',
]

extras_require = {
    'docs': [
        'Sphinx>=1.5.1',
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for reqs in extras_require.values():
    extras_require['all'].extend(reqs)

setup_requires = [
    'Babel>=1.3',
    'pytest-runner>=2.6.2',
]

install_requires = [
    'Flask-BabelEx>=0.9.2',
    'click>=6.7',
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
    keywords='SDSC TODO',
    license='GPLv2',
    author='Swiss Data Science Center',
    author_email='contact@datascience.ch',
    url='https://github.com/SwissDataScienceCenter/renga-python',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': ['renga=renga.cli:cli'],
        'renga.cli': ['init=renga.cli:init'],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: TO-DO',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Development Status :: 1 - Planning',
    ], )
