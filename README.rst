..
    Copyright 2017 - Swiss Data Science Center (SDSC)
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
 Renga CLI and SDK for Python
==============================

.. image:: https://img.shields.io/travis/SwissDataScienceCenter/renga-python.svg
   :target: https://travis-ci.org/SwissDataScienceCenter/renga-python

.. image:: https://img.shields.io/coveralls/SwissDataScienceCenter/renga-python.svg
   :target: https://coveralls.io/r/SwissDataScienceCenter/renga-python

.. image:: https://img.shields.io/github/tag/SwissDataScienceCenter/renga-python.svg
   :target: https://github.com/SwissDataScienceCenter/renga-python/releases

.. image:: https://img.shields.io/pypi/dm/renga.svg
   :target: https://pypi.python.org/pypi/renga

.. image:: http://readthedocs.org/projects/renga-python/badge/?version=latest
   :target: http://renga-python.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/github/license/SwissDataScienceCenter/renga-python.svg
        :target: https://github.com/SwissDataScienceCenter/renga-python/blob/master/LICENSE

A Python library for the `Renga collaborative data science platform
<https://github.com/SwissDataScienceCenter/renga>`_. It lets you perform any action with
``renga`` command or from withing Python apps - create projects, manage
buckets, track files, run containers, etc.


**NOTE**:
   ``renga-python`` is the python library for Renga that provides an SDK and a 
   command-line interface (CLI). It *does not* start the Renga platform itself - 
   for that, refer to the Renga docs on `running the platform 
   <https://renga.readthedocs.io/en/latest/user/setup.html>`_.

**This is an experimental developer preview release.**

Installation
------------

The latest release is available on PyPI and can be installed using
``pip``:

::

    $ pip install renga

The development version can be installed directly from the Git repository:

::

    $ pip install -e git+https://github.com/SwissDataScienceCenter/renga-python.git#egg=renga


Usage
-----

Start by login and configuring your shell:

::

   $ renga login http://localhost

Connect to Renga platform from Python using the stored configuration:

::

    import renga
    client = renga.from_config()

The client can be also instantiated from environment variables.

::

   $ eval "$(renga env)"

::

   import renga
   client = renga.from_env()


Further documentation is available on
https://renga-python.readthedocs.io/
