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

Tools and Workflows
===================

.. py:module:: renku.models.cwl

Manage creation of tools and workflows using the `Common Workflow Language
<http://www.commonwl.org/>`_ (CWL).


Common Workflow language
------------------------

Renku uses CWL to represent runnable steps (tools) along with their inputs
and outputs. Similarly, tools can be chained together to form CWL-defined
workflows.


Command-line tool
~~~~~~~~~~~~~~~~~

.. automodule:: renku.models.cwl.command_line_tool
   :members:

Parameter
~~~~~~~~~

.. automodule:: renku.models.cwl.parameter
   :members:

Process
~~~~~~~

.. automodule:: renku.models.cwl.process
   :members:

Types
~~~~~

.. automodule:: renku.models.cwl.types
   :members:

Workflow
~~~~~~~~

.. automodule:: renku.models.cwl.workflow
   :members:
