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

Renku Core Service
==================

The Renku Core service exposes a functionality similar to the Renku CLI via a
JSON-RPC API.

Components:

.. uml::

    @startuml
    skinparam componentStyle rectangle
    skinparam lineStyle ortho

    component Frontend {
        [Views] -down-> RequestModel
        ResponseModel -up-> [Views]
    }

    component "Business Logic" as bl {
        [Controllers] --> renku.core
    }

    database "External Git" as git {
        folder "Remote Projects" as rp
    }

    database "Redis" as redis {
        frame "Metadata" as metadata
        frame "Jobs" as jobs
    }

    database "File Cache" as filecache {
        folder "Projects" as projects
        folder "Uploaded Files" as files
    }

    RequestModel -down-> bl
    bl -up-> ResponseModel
    [Controllers] -right-> rp
    [Workers] -right-> bl
    bl -left-> [Workers]
    [Workers] -down-> jobs
    bl -down-> filecache
    bl -down-> redis

    projects -[hidden]down-> files
    metadata -[hidden]down-> jobs
    @enduml


API Specification
-----------------

To explore the API documentation and test the current API against a running
instance of Renku you can use the `Swagger UI on renkulab.io
<https://renkulab.io/swagger>`_.


Errors
------

You can check the error details here

.. toctree::
   service_errors
