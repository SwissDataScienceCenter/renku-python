..
    Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

Getting Started
===============
.. _gettingstarted-reference:

Interaction with the platform can take place via the command-line
interface (CLI).

Start by creating for folder where you want to keep your Renku project:

.. code-block:: console

   $ renku init my-renku-project
   $ cd my-renku-project

Create a dataset and add data to it:

.. code-block:: console

   $ renku dataset create my-dataset
   $ renku dataset add my-dataset https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/README.rst

Run an analysis:

.. code-block:: console

   $ renku run wc < data/my-dataset/README.rst > wc_readme

Trace the data provenance:

.. code-block:: console

    $ renku log wc_readme

These are the basics, but there is much more that Renku allows you to do with
your data analysis workflows.

For more information about using `renku`, refer to the `renku --help`.
