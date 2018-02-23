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

Client
======
.. py:module:: renga.client

Creating a client
-----------------

There are several ways to instantiate a client used for communication with
the Renga platform.

1. The easiest way is by calling the function :py:func:`~renga.client.from_env`
   when running in an environment created by the Renga platform itself.
2. The client can be created from a local configuration file by calling
   :py:func:`~renga.cli._client.from_config`.
3. Lastly, it can also be configured manually by
   instantiating a :py:class:`~renga.client.RengaClient` class.

.. autofunction:: from_env()

.. autofunction:: renga.cli._client.from_config()

Client reference
----------------

.. autoclass:: RengaClient()

