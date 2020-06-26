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

Renku Workflow
==============

.. py:module:: renku.core.models.workflow

Renku uses PROV-O and its own Renku ontology to represent workflows.


Run
---

.. automodule:: renku.core.models.workflow.run
   :members:

Parameters
----------

.. automodule:: renku.core.models.workflow.parameters
   :members:

Renku Workflow Conversion
=========================

.. py:module:: renku.core.models.workflow.converters

Renku allows conversion of tracked workflows to runnable workflows in
supported tools (Currently CWL)

CWL
---

.. automodule:: renku.core.models.workflow.converters.cwl
   :no-members:

.. autoclass:: CWLConverter
   :members:
   :inherited-members:
