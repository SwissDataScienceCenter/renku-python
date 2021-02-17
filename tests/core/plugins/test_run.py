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
"""Test plugins for the ``run`` command."""

from renku.cli import cli
from renku.core.plugins import pluginmanager as pluginmanager


def test_renku_pre_run_hook(monkeypatch, dummy_pre_run_plugin_hook, runner, project):
    """Tests that the renku run plugin hook on ``CmdLineTool`` is called."""
    pm = pluginmanager.get_plugin_manager()
    pm.register(dummy_pre_run_plugin_hook)

    with monkeypatch.context() as m:
        m.setattr(pluginmanager, "get_plugin_manager", lambda: pm)
        cmd = ["echo", "test"]

        result = runner.invoke(cli, ["run", "--no-output"] + cmd)

        assert 0 == result.exit_code
        assert 1 == dummy_pre_run_plugin_hook.called


def test_renku_run_cwl_hook(monkeypatch, dummy_run_plugin_hook, runner, project):
    """Tests that the renku run plugin hook on ``CmdLineTool`` is called."""
    pm = pluginmanager.get_plugin_manager()
    pm.register(dummy_run_plugin_hook)

    with monkeypatch.context() as m:
        m.setattr(pluginmanager, "get_plugin_manager", lambda: pm)
        cmd = ["echo", "test"]
        result = runner.invoke(cli, ["run", "--no-output"] + cmd)
        assert 0 == result.exit_code

        # check for dummy plugin
        result = runner.invoke(cli, ["log", "--format", "json-ld"])
        assert "Dummy Cmdline Hook" in result.output
        assert "dummy cmdline hook body" in result.output


def test_renku_processrun_cwl_hook(monkeypatch, dummy_processrun_plugin_hook, runner, project):
    """Tests that the renku run plugin hook on ``ProcessRun`` is called."""
    pm = pluginmanager.get_plugin_manager()
    pm.register(dummy_processrun_plugin_hook)

    with monkeypatch.context() as m:
        m.setattr(pluginmanager, "get_plugin_manager", lambda: pm)
        cmd = ["echo", "test"]
        result = runner.invoke(cli, ["run", "--no-output"] + cmd)
        assert 0 == result.exit_code

        # check for dummy plugin
        result = runner.invoke(cli, ["log", "--format", "json-ld"])
        assert "Dummy ProcessRun Hook" in result.output
        assert "dummy ProcessRun hook body" in result.output
