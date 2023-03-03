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


Core Business Logic
===================

``renku.core`` contains the business logic of renku-python. Functionality is
split into subfolders based on topic, such as ``dataset`` or ``workflow``.

Command Builder
---------------

Most renku commands require context (database/git/etc.) to be set up for them.
The command builder pattern makes this easy by wrapping commands in factory
methods.

.. automodule:: renku.command.command_builder
   :members:

JSON-LD Schemes
---------------

Schema classes used to serialize domain models to JSON-LD.

.. automodule:: renku.command.schema.activity
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.agent
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.annotation
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.calamus
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.composite_plan
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.dataset
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.entity
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.parameter
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.plan
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.project
   :members:
   :show-inheritance:

.. automodule:: renku.command.schema.workflow_file
   :members:
   :show-inheritance:


Datasets
--------

.. automodule:: renku.core.dataset.dataset
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.dataset_add
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.context
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.datasets_provenance
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.pointer_file
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.request_model
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.tag
   :members:
   :show-inheritance:


Dataset Providers
-----------------

Providers for dataset import and export

.. automodule:: renku.core.dataset.providers.api
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.dataverse
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.dataverse_metadata_templates
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.doi
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.git
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.local
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.models
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.olos
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.renku
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.repository
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.web
   :members:
   :show-inheritance:

.. automodule:: renku.core.dataset.providers.zenodo
   :members:
   :show-inheritance:

Workflows
---------

.. automodule:: renku.core.workflow.activity
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.plan
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.execute
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.model.concrete_execution_graph
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.model.workflow_file
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.plan_factory
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.types
   :members:

.. automodule:: renku.core.workflow.value_resolution
   :members:
   :show-inheritance:

.. automodule:: renku.core.workflow.workflow_file
   :members:
   :show-inheritance:


Sessions
--------

.. automodule:: renku.core.session.docker
   :members:
   :show-inheritance:

.. automodule:: renku.core.session.session
   :members:
   :show-inheritance:

Templates
---------

.. automodule:: renku.core.template.template
   :members:
   :show-inheritance:

.. automodule:: renku.core.template.usecase
   :members:
   :show-inheritance:


Errors
------
Errors that can be raised by ``renku.core``.

.. automodule:: renku.core.errors
   :members:
   :show-inheritance:

Utilities
---------

.. automodule:: renku.core.util.communication
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.contexts
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.datetime8601
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.doi
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.file_size
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.git
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.metadata
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.os
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.requests
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.shacl
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.urls
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.util
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.uuid
   :members:
   :show-inheritance:

.. automodule:: renku.core.util.yaml
   :members:
   :show-inheritance:

Git Internals
-------------

.. automodule:: renku.core.git
   :members:

.. automodule:: renku.domain_model.git
   :members:
