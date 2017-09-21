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


.. include:: ../README.rst
   :end-before: Usage

For more information about the Renga API `see its documentation
<https://renga.readthedocs.org/latest/developer/index.html>`_.

Getting started
---------------

To instantiate a Renga client from a running notebook on the platform, you
can use :py:func:`~renga.client.from_env` helper function.

.. code-block:: python

   import renga
   client = renga.from_env()

You can now upload files to new bucket:

.. code-block:: python

   >>> bucket = client.buckets.create('first-bucket')
   >>> with bucket.files.open('greeting.txt', 'w') as fp:
   ...     fp.write('hello world')

You can access files from a bucket:

.. code-block:: python

   >>> client.buckets.list()[0].files.list()[0].open('r').read()
   b'hello world'


For more details and examples have a look at :doc:`the reference
<client>`.


.. toctree::
   :hidden:
   :maxdepth: 2

   client
   projects
   buckets
   contexts
   api
   contributing
   changes
   license
   authors
