..
    Copyright 2017-2020 - Swiss Data Science Center (SDSC)
    A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
    Eidgenössische Technische Hochschule Zürich (ETHZ).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

==============================
 Renku CLI and SDK for Python
==============================

.. image:: https://github.com/SwissDataScienceCenter/renku-python/workflows/Test,%20Integration%20Tests%20and%20Deploy/badge.svg
   :target: https://github.com/SwissDataScienceCenter/renku-python/actions?query=workflow%3A%22Test%2C+Integration+Tests+and+Deploy%22+branch%3Amaster

.. image:: https://img.shields.io/coveralls/SwissDataScienceCenter/renku-python.svg
   :target: https://coveralls.io/r/SwissDataScienceCenter/renku-python

.. image:: https://img.shields.io/github/tag/SwissDataScienceCenter/renku-python.svg
   :target: https://github.com/SwissDataScienceCenter/renku-python/releases

.. image:: https://img.shields.io/pypi/dm/renku.svg
   :target: https://pypi.python.org/pypi/renku

.. image:: http://readthedocs.org/projects/renku-python/badge/?version=latest
   :target: http://renku-python.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/github/license/SwissDataScienceCenter/renku-python.svg
   :target: https://github.com/SwissDataScienceCenter/renku-python/blob/master/LICENSE

.. image:: https://pullreminders.com/badge.svg
   :target: https://pullreminders.com?ref=badge
   :alt: Pull reminders

A Python library for the `Renku collaborative data science platform
<https://github.com/SwissDataScienceCenter/renku>`_. It allows the user to
create projects, manage datasets, and capture data provenance while performing
analysis tasks.

**NOTE**:
   ``renku-python`` is the python library for Renku that provides an SDK and a
   command-line interface (CLI). It *does not* start the Renku platform itself -
   for that, refer to the Renku docs on `running the platform
   <https://renku.readthedocs.io/en/latest/developer/setup.html>`_.

Installation
============

Renku releases and development versions are available from `PyPI
<https://pypi.org/project/renku/>`_. You can install it using any tool that
knows how to handle PyPI packages. Our recommendation is to use `:code:pipx
<https://github.com/pipxproject/pipx>`_.


``pipx``
--------

First, `install pipx <https://github.com/pipxproject/pipx#install-pipx>`_
and make sure that the ``$PATH`` is correctly configured.

::

    $ python3 -m pip install --user pipx
    $ pipx ensurepath

Once ``pipx`` is installed use following command to install ``renku``.

::

    $ pipx install renku
    $ which renku
    ~/.local/bin/renku


``pipx`` installs renku into its own virtual environment, making sure that it
does not pollute any other packages or versions that you may have already
installed.

.. note::

    If you install renku as a dependency in a virtual environment and the
    environment is active, your shell will default to the version installed
    in the virtual environment, *not* the version installed by ``pipx``.


To install a development release:

::

    $ pipx install --pip-args pre renku


``pip``
-------

::

    $ pip install renku

The latest development versions are available on PyPI or from the Git
repository:

::

    $ pip install --pre renku
    # - OR -
    $ pip install -e git+https://github.com/SwissDataScienceCenter/renku-python.git#egg=renku

Use following installation steps based on your operating system and preferences
if you would like to work with the command line interface and you do not need
the Python library to be importable.


Docker
------

The containerized version of the CLI can be launched using Docker command.

::

    $ docker run -it -v "$PWD":"$PWD" -w="$PWD" renku/renku-python renku

It makes sure your current directory is mounted to the same place in the
container.


Usage
=====

Initialize a renku project:

::

    $ mkdir -p ~/temp/my-renku-project
    $ cd ~/temp/my-renku-project
    $ renku init

Create a dataset and add data to it:

::

    $ renku dataset create my-dataset
    $ renku dataset add my-dataset https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/README.rst

Run an analysis:

::

    $ renku run wc < data/my-dataset/README.rst > wc_readme

Trace the data provenance:

::

    $ renku log wc_readme

These are the basics, but there is much more that Renku allows you to do with
your data analysis workflows. The full documentation will soon be available
at: https://renku-python.readthedocs.io/


Developing Renku
================

For development it's convenient to install ``renku`` in editable mode. This is
still possible with ``pipx``. First clone the repository and then do:

::

    $ pipx install \
        --editable \
        <path-to-renku-python>[all] \
        renku

This will install all the extras for testing and debugging.


Run tests
---------

To run tests locally with specific version of Python:

::

    $ pyenv install 3.7.5rc1
    $ pipenv --python ~/.pyenv/versions/3.7.5rc1/bin/python install
    $ pipenv run tests


To recreate environment with different version of Python, it's easy to do so with the following commands:

::

    $ pipenv --rm
    $ pyenv install 3.6.9
    $ pipenv --python ~/.pyenv/versions/3.6.9/bin/python install
    $ pipenv run tests



Using External Debuggers
------------------------

To run ``renku`` via e.g. the `Visual Studio Code debugger
<https://code.visualstudio.com/docs/python/debugging>`_ you need run it via
the python executable in whatever virtual environment was used to install ``renku``. If there is a package
needed for the debugger, you need to inject it into the virtual environment first, e.g.:

::

    $ pipx inject renku ptvsd


Finally, run ``renku`` via the debugger:

::

    $ ~/.local/pipx/venvs/renku/bin/python -m ptvsd --host localhost --wait -m renku.cli <command>


If using Visual Studio Code, you may also want to set the ``Remote Attach`` configuration
``PathMappings`` so that it will find your source code, e.g.

::

    {
            "name": "Python: Remote Attach",
            "type": "python",
            "request": "attach",
            "port": 5678,
            "host": "localhost",
            "pathMappings": [
                {
                    "localRoot": "<path-to-renku-python-source-code>",
                    "remoteRoot": "<path-to-renku-python-source-code>"
                }
            ]
        },
