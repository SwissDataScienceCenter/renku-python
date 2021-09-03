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

Renku Workflow
==============

.. py:module:: renku.core.models.workflow

Renku uses PROV-O and its own Renku ontology to represent workflows.


Plans
-----

.. automodule:: renku.core.models.workflow.plan
   :members:


.. automodule:: renku.core.models.workflow.composite_plan
   :members:

Parameters
----------

.. automodule:: renku.core.models.workflow.parameter
   :members:


Renku Workflow Logic
====================

.. py:module:: renku.core.management.workflow

Execution Graph
---------------

.. automodule:: renku.core.management.workflow.concrete_execution_graph
   :members:

Value Resolution
----------------

.. automodule:: renku.core.management.workflow.value_resolution
   :members:

Plan Factory
------------

Used to create ``Plan`` objects based on command line arguments

.. automodule:: renku.core.management.workflow.plan_factory
   :members:



Renku Workflow Conversion
=========================

.. py:module:: renku.core.management.workflow.converters

Renku allows conversion of tracked workflows to runnable workflows in
supported tools (Currently CWL)

CWL
---

.. automodule:: renku.core.management.workflow.converters.cwl
   :no-members:

.. autoclass:: CWLConverter
   :members:
   :inherited-members:
