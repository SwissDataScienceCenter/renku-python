# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test plugins for the ``workflow`` command."""

from renku.cli import cli
from renku.core.plugins import pluginmanager as pluginmanager


def test_renku_workflow_exporter_hook(monkeypatch, dummy_workflow_exporter_hook, runner, project):
    """Tests that the renku workflow export plugin hook on ``Plan`` is called."""
    pm = pluginmanager.get_plugin_manager()
    pm.register(dummy_workflow_exporter_hook)

    with monkeypatch.context() as m:
        m.setattr(pluginmanager, "get_plugin_manager", lambda: pm)

        cmd = ["echo", "test"]
        run_name = "run_1"
        result = runner.invoke(cli, ["run", "--no-output", "--name", run_name] + cmd)
        assert 0 == result.exit_code

        result = runner.invoke(cli, ["workflow", "export", "--format", "dummy", run_name])
        assert "dummy" in result.output
