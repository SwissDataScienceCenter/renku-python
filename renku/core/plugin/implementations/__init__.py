# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Renku plugin implementations."""

from typing import TYPE_CHECKING, List, Type

from renku.core.dataset.providers.dataverse import DataverseProvider
from renku.core.dataset.providers.git import GitProvider
from renku.core.dataset.providers.local import FilesystemProvider
from renku.core.dataset.providers.olos import OLOSProvider
from renku.core.dataset.providers.renku import RenkuProvider
from renku.core.dataset.providers.s3 import S3Provider
from renku.core.dataset.providers.web import WebProvider
from renku.core.dataset.providers.zenodo import ZenodoProvider
from renku.core.session.docker import DockerSessionProvider
from renku.core.session.renkulab import RenkulabSessionProvider
from renku.core.workflow.converters.cwl import CWLExporter
from renku.core.workflow.converters.renku import RenkuWorkflowFileExporter
from renku.core.workflow.parser.renku import RenkuWorkflowFileParser
from renku.core.workflow.providers.cwltool import CWLToolProvider
from renku.core.workflow.providers.local import LocalWorkflowProvider

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi
    from renku.core.interface.workflow_file_parser import IWorkflowFileParser
    from renku.domain_model.session import ISessionProvider
    from renku.domain_model.workflow.converters import IWorkflowConverter
    from renku.domain_model.workflow.provider import IWorkflowProvider

__all__: List[str] = []

session_providers: "List[Type[ISessionProvider]]" = [DockerSessionProvider, RenkulabSessionProvider]
workflow_exporters: "List[Type[IWorkflowConverter]]" = [CWLExporter, RenkuWorkflowFileExporter]
workflow_providers: "List[Type[IWorkflowProvider]]" = [CWLToolProvider, LocalWorkflowProvider]
dataset_providers: "List[Type[ProviderApi]]" = [
    DataverseProvider,
    GitProvider,
    FilesystemProvider,
    OLOSProvider,
    RenkuProvider,
    S3Provider,
    WebProvider,
    ZenodoProvider,
]
workflow_file_parsers: "List[Type[IWorkflowFileParser]]" = [RenkuWorkflowFileParser]

try:
    from renku.core.workflow.providers.toil import ToilProvider

    workflow_providers.append(ToilProvider)
except ImportError:
    pass
