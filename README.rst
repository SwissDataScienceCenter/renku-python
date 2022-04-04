..
    Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

======================================
 Renku Python Library, CLI and Service
======================================

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

A Python library for the `Renku collaborative data science platform
<https://github.com/SwissDataScienceCenter/renku>`_. It includes a CLI and SDK
for end-users as well as a service backend. It provides functionality for the
creation and management of projects and datasets, and simple utilities to
capture data provenance while performing analysis tasks.

**NOTE**:
   ``renku-python`` is the python library and core service for Renku - it *does
   not* start the Renku platform itself - for that, refer to the Renku docs on
   `running the platform
   <https://renku.readthedocs.io/en/latest/developer/setup.html>`_.


Renku for Users
===============

Installation
------------

.. _installation-reference:

Renku releases and development versions are available from `PyPI
<https://pypi.org/project/renku/>`_. You can install it using any tool that
knows how to handle PyPI packages. Our recommendation is to use `:code:pipx
<https://github.com/pipxproject/pipx>`_.

.. note::

   We do not officially support Windows at this moment. The way Windows
   handles paths and symlinks interferes with some Renku functionality.
   We recommend using the Windows Subsystem for Linux (WSL) to use Renku
   on Windows.

Prerequisites
~~~~~~~~~~~~~

Renku depends on Git under the hood, so make sure that you have Git
`installed on your system <https://git-scm.com/downloads>`_.

Renku also offers support to store large files in `Git LFS
<https://git-lfs.github.com/>`_, which is used by default and should be
installed on your system. If you do not wish to use Git LFS, you can run
Renku commands with the `-S` flag, as in `renku -S <command>`.  More
information on Git LFS usage in renku can be found in the `Data in Renku
<https://renku.readthedocs.io/en/latest/user/data.html>`_ section of the docs.

Renku uses CWL to execute recorded workflows when calling `renku update`
or `renku rerun`. CWL depends on NodeJs to execute the workflows, so installing
`NodeJs <https://nodejs.org/en/download/package-manager/>`_ is required if
you want to use those features.

For development of the service, `Docker <https://docker.com>`_ is recommended.


.. _pipx-before-reference:

``pipx``
~~~~~~~~
.. _pipx-after-reference:

First, `install pipx <https://github.com/pipxproject/pipx#install-pipx>`_
and make sure that the ``$PATH`` is correctly configured.

::

    $ python3 -m pip install --user pipx
    $ python3 -m pipx ensurepath

Once ``pipx`` is installed use following command to install ``renku``.

::

    $ pipx install renku
    $ which renku
    ~/.local/bin/renku


``pipx`` installs Renku into its own virtual environment, making sure that it
does not pollute any other packages or versions that you may have already
installed.

.. note::

    If you install Renku as a dependency in a virtual environment and the
    environment is active, your shell will default to the version installed
    in the virtual environment, *not* the version installed by ``pipx``.


To install a development release:

::

    $ pipx install --pip-args pre renku


.. _pip-before-reference:

``pip``
~~~~~~~
.. _pip-after-reference:

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

.. _windows-before-reference:

Windows
~~~~~~~
.. _windows-after-reference:

.. note::

    We don't officially support Windows yet, but Renku works well in the Windows Subsystem for Linux (WSL).
    As such, the following can be regarded as a best effort description on how to get started with Renku on Windows.

Renku can be run using the Windows Subsystem for Linux (WSL). To install the WSL, please follow the
`official instructions <https://docs.microsoft.com/en-us/windows/wsl/install-win10#manual-installation-steps>`__.

We recommend you use the Ubuntu 20.04 image in the WSL when you get to that step of the installation.

Once WSL is installed, launch the WSL terminal and install the packages required by Renku with:

::

    $ sudo apt-get update && sudo apt-get install git python3 python3-pip python3-venv pipx

Since Ubuntu has an older version of git LFS installed by default which is known to have some bugs when cloning
repositories, we recommend you manually install the newest version by following
`these instructions <https://github.com/git-lfs/git-lfs/wiki/Installation#debian-and-ubuntu>`__.

Once all the requirements are installed, you can install Renku normally by running:

::

    $ pipx install renku
    $ pipx ensurepath

After this, Renku is ready to use. You can access your Windows in the various mount points in
``/mnt/`` and you can execute Windows executables (e.g. ``\*.exe``) as usual directly from the
WSL (so ``renku run myexecutable.exe`` will work as expected).

.. _docker-before-reference:

Docker
~~~~~~
.. _docker-after-reference:

The containerized version of the CLI can be launched using Docker command.

::

    $ docker run -it -v "$PWD":"$PWD" -w="$PWD" renku/renku-python renku

It makes sure your current directory is mounted to the same place in the
container.


CLI Example
-----------

Initialize a Renku project:

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

    $ renku run --name my-workflow -- wc < data/my-dataset/README.rst > wc_readme

Trace the data provenance:

::

    $ renku workflow visualize wc_readme

These are the basics, but there is much more that Renku allows you to do with
your data analysis workflows. The full documentation will soon be available
at: https://renku-python.readthedocs.io/


Renku as a Service
==================

This repository includes a ``renku-core`` RPC service written as a `Flask
<https://flask.palletsprojects.com>`_ application that provides (almost) all of
the functionality of the Renku CLI. This is used to provide one of the backends
for the `RenkuLab <https://renkulab.io>`_ web UI. The service can be deployed in
production as a Helm chart (see `helm-chart <./helm-chart/README.rst>`_.


Deploying locally
-----------------

To test the service functionality you can deploy it quickly and easily using
``docker-compose up`` [docker-compose](https://pypi.org/project/docker-compose/).
Make sure to make a copy of the ``renku/service/.env-example`` file and configure it
to your needs. The setup here is to expose the service behind a traefik reverse proxy
to mimic an actual production deployment. You can access the proxied endpoints at
``http://localhost/api``. The service itself is exposed on port 8080 so its endpoints
are available directly under ``http://localhost:8080``.


API Documentation
-----------------

The renku core service implements the API documentation as an OpenAPI 3.0.x spec.
You can retrieve the yaml of the specification itself with

```
$ renku service apispec
```

If deploying the service locally with ``docker-compose`` you can find the swagger-UI
under ``localhost/api/swagger``. To send the proper authorization headers to the
service endpoints, click the ``Authorize`` button and enter a valid JWT token and
a gitlab token with read/write repository scopes. The JWT token can be obtained by
logging in to a renku instance with ``renku login`` and retrieving it from your local
renku configuration.

In a live deployment, the swagger documentation is available under ``https://<renku-endpoint>/swagger``.
You can authorize the API by first logging into renku normally, then going to the
swagger page, clicking ``Authorize`` and picking the ``oidc (OAuth2, authorization_code)``
option. Leave the ``client_id`` as ``swagger`` and the ``client_secret`` empty, select
all scopes and click ``Authorize``. You should now be logged in and you can send
requests using the ``Try it out`` buttons on individual requests.


Developing Renku
================

For testing the functionality from source it is convenient to install ``renku``
in editable mode using ``pipx``. Clone the repository and then do:

::

    $ pipx install \
        --editable \
        <path-to-renku-python>[all] \
        renku

This will install all the extras for testing and debugging.

If you already use `pyenv <https://github.com/pyenv/pyenv>`__ to manage different python versions,
you may be interested in installing `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`__ to
create an ad-hoc virtual environment for developing renku.

Once you have created and activated a virtual environment for renku-python, you can use the usual
`pip` commands to install the required dependencies.

::

    $ pip install -e .[all]  # use `.[all]` for zsh


Service
-------

Developing the service and testing its APIs can be done with ``docker compose`` (see
"Deploying Locally" above).

If you have a full RenkuLab deployment at your disposal, you can
use `telepresence <https://www.telepresence.io/>`__ v1 to develop and debug locally.
Just run the `start-telepresence.sh` script and follow the instructions.
Mind that the script doesn't work with telepresence v2.


Running tests
-------------

We use `pytest <https://docs.pytest.org>`__ for running tests.
You can use our `run-tests.sh` script for running specific set of tests.

::

    $ ./run-tests.sh -h

We lint the files using `black <https://github.com/psf/black>`__ and
`isort <https://github.com/PyCQA/isort>`__.


Using External Debuggers
------------------------

Local Machine
~~~~~~~~~~~~~

To run ``renku`` via e.g. the `Visual Studio Code debugger
<https://code.visualstudio.com/docs/python/debugging>`_ you need run it via
the python executable in whatever virtual environment was used to install ``renku``. If there is a package
needed for the debugger, you need to inject it into the virtual environment first, e.g.:

::

    $ pipx inject renku ptvsd


Finally, run ``renku`` via the debugger:

::

    $ ~/.local/pipx/venvs/renku/bin/python -m ptvsd --host localhost --wait -m renku.ui.cli <command>


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
    }
