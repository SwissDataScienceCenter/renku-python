# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku CLI fixtures for execution management."""

import pytest


@pytest.fixture
def renku_cli(client, run):
    """Return a callable Renku CLI.

    It returns the exit code and content of the resulting CWL tool.
    """
    import yaml

    from renku.core.models.provenance.activities import Activity

    def renku_cli_(*args, **kwargs):
        before_wf_files = set(client.workflow_path.glob("*.yaml"))
        exit_code = run(args, **kwargs)
        after_wf_files = set(client.workflow_path.glob("*.yaml"))
        new_files = after_wf_files - before_wf_files
        assert len(new_files) <= 1
        if new_files:
            wf_filepath = new_files.pop()
            with wf_filepath.open("r") as f:
                content = Activity.from_jsonld(yaml.safe_load(f), client=client, commit=client.repo.head.commit)
            content = content.association.plan
        else:
            content = None

        return exit_code, content

    return renku_cli_
