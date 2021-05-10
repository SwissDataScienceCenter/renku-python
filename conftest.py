# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 Swiss Data Science Center (SDSC)
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
"""Pytest configuration."""
import importlib

CLI_FIXTURE_LOCATIONS = [
    "tests.cli.fixtures.cli_gateway",
    "tests.cli.fixtures.cli_kg",
    "tests.cli.fixtures.cli_old_projects",
    "tests.cli.fixtures.cli_projects",
    "tests.cli.fixtures.cli_providers",
    "tests.cli.fixtures.cli_repository",
    "tests.cli.fixtures.cli_runner",
]

CORE_FIXTURE_LOCATIONS = [
    "tests.core.fixtures.core_datasets",
    "tests.core.fixtures.core_plugins",
    "tests.core.fixtures.core_projects",
    "tests.core.fixtures.core_serialization",
]

GLOBAL_FIXTURE_LOCATIONS = [
    "tests.fixtures.common",
    "tests.fixtures.config",
    "tests.fixtures.graph",
    "tests.fixtures.repository",
    "tests.fixtures.runners",
    "tests.fixtures.templates",
]

SERVICE_FIXTURE_LOCATIONS = [
    "tests.service.fixtures.service_cache",
    "tests.service.fixtures.service_client",
    "tests.service.fixtures.service_controllers",
    "tests.service.fixtures.service_data",
    "tests.service.fixtures.service_endpoints",
    "tests.service.fixtures.service_integration",
    "tests.service.fixtures.service_jobs",
    "tests.service.fixtures.service_projects",
    "tests.service.fixtures.service_scheduler",
]

INCLUDE_FIXTURES = GLOBAL_FIXTURE_LOCATIONS + CORE_FIXTURE_LOCATIONS + CLI_FIXTURE_LOCATIONS + SERVICE_FIXTURE_LOCATIONS


for _fixture in INCLUDE_FIXTURES:
    module = importlib.import_module(_fixture)
    globals().update(
        {n: getattr(module, n) for n in module.__all__}
        if hasattr(module, "__all__")
        else {k: v for (k, v) in module.__dict__.items() if not k.startswith("_")}
    )
