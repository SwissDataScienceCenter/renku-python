..
    Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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

.. image:: https://img.shields.io/travis/SwissDataScienceCenter/renku-python.svg
   :target: https://travis-ci.org/SwissDataScienceCenter/renku-python

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
<https://github.com/SwissDataScienceCenter/renku>`_. It allows the user to
create projects, manage datasets, and capture data provenance while performing
analysis tasks.

**NOTE**:
   ``renku-python`` is the python library for Renku that provides an SDK and a
   command-line interface (CLI). It *does not* start the Renku platform itself -
   for that, refer to the Renku docs on `running the platform
   <https://renku.readthedocs.io/en/latest/user/setup.html>`_.

Installation
------------

The latest release is available on PyPI and can be installed using
``pip``:

::

    $ pip install renku

The latest code can be installed directly from the Git repository:

::

    $ pip install -e git+https://github.com/SwissDataScienceCenter/renku-python.git#egg=renku


Usage
-----

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
