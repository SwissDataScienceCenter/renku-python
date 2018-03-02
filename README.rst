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
<https://github.com/SwissDataScienceCenter/renga>`_. It allows the user to
create projects, manage datasets, and capture data provenance while performing
analysis tasks.


**NOTE**:
   ``renga-python`` is the python library for Renga that provides an SDK and a
   command-line interface (CLI). It *does not* start the Renga platform itself -
   for that, refer to the Renga docs on `running the platform
   <https://renga.readthedocs.io/en/latest/user/setup.html>`_.

**This is the development branch of `renga-python` and should be considered
highly volatile. The documentation for certain components may be out of
sync.**

Installation
------------

The latest release is available on PyPI and can be installed using
``pip``:

::

    $ pip install renga

The development branch can be installed directly from the Git repository:

::

    $ pip install -e git+https://github.com/SwissDataScienceCenter/renga-python.git@development#egg=renga


Usage
-----

Initialize a renga project:

::

    $ mkdir -p ~/temp/my-renga-project
    $ cd ~/temp/my-renga-project
    $ renga init

Create a dataset and add data to it:

::

    $ renga datasets create my-dataset
    $ renga datasets add my-dataset https://raw.githubusercontent.com/SwissDataScienceCenter/renga-python/development/README.rst

Run an analysis:

::

    $ renga run wc data/my-dataset/README.rst > wc_readme

Trace the data provenance:

::

    $ renga log wc_readme

These are the basics, but there is much more that renga allows you to do with
your data analysis workflows. The full documentation will soon be available
at: https://renga-python.readthedocs.io/en/development


Contributing
------------

We're happy to receive contributions of all kinds, whether it is an idea for a
new feature, a bug report or a pull request.

Before you submit a pull request, please run

::

   $ yapf -irp .
   $ ./run-tests.sh

You may want to set up yapf styling as a pre-commit hook to do this
automatically:

::

   curl https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh -o .git/hooks/pre-commit
   chmod u+x .git/hooks/pre-commit


.. _yapf: https://github.com/google/yapf/
