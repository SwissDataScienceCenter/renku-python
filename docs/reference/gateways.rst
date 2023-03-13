..
    Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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


Gateways
========

Renku uses several gateways to abstract away dependencies on external systems
such as the database or git.

Interfaces
----------

Interfaces that the Gateways implement.

.. automodule:: renku.core.interface.activity_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.core.interface.database_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.core.interface.dataset_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.core.interface.storage
   :members:
   :show-inheritance:

.. automodule:: renku.core.interface.plan_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.core.interface.project_gateway
   :members:
   :show-inheritance:

Implementations
---------------

Implementation of Gateway interfaces.

.. automodule:: renku.infrastructure.gateway.activity_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.gateway.database_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.gateway.dataset_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.storage.factory
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.storage.rclone
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.gateway.plan_gateway
   :members:
   :show-inheritance:

.. automodule:: renku.infrastructure.gateway.project_gateway
   :members:
   :show-inheritance:


Repository
----------

Renku uses git repositories for tracking changes. To abstract away git internals,
we delegate all git calls to the ``Repository`` class.

.. automodule:: renku.infrastructure.repository
   :members:
   :show-inheritance:
