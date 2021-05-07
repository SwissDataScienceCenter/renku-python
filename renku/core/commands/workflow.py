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
"""Renku workflow commands."""


from pathlib import Path

from renku.core import errors
from renku.core.commands.format.workflows import WORKFLOWS_FORMATS
from renku.core.commands.graph import Graph
from renku.core.incubation.command import Command
from renku.core.models.workflow.converters.cwl import CWLConverter


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


def _list_workflows(client, format=None, columns=None):
    """List or manage workflows with subcommands."""
    plans = client.dependency_graph.plans

    if format is None:
        return list(plans)

    if format not in WORKFLOWS_FORMATS:
        raise errors.UsageError("format not supported")

    return WORKFLOWS_FORMATS[format](client, plans, columns=columns)


def list_workflows_command():
    """Command to list or manage workflows with subcommands."""
    return Command().command(_list_workflows).require_migration()


def _set_workflow_name(client, name, path, force):
    """Sets the <name> for remote <path>."""
    from renku.core.models.refs import LinkReference

    LinkReference.create(client=client, name=_ref(name), force=force).set_reference(path)


def set_workflow_name_command():
    """Command that sets the <name> for remote <path>."""
    return Command().command(_set_workflow_name).require_clean().with_commit()


def _rename_workflow(client, old, new, force):
    """Rename the workflow named <old> to <new>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(old)).rename(_ref(new), force=force)


def rename_workflow_command():
    """Command that renames the workflow named <old> to <new>."""
    return Command().command(_rename_workflow).require_clean().with_commit()


def _remove_workflow(client, name):
    """Remove the remote named <name>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(name)).delete()


def remove_workflow_command():
    """Command that removes the remote named <name>."""
    return Command().command(_remove_workflow).require_clean().with_commit()


def _show_workflow(client, name_or_id):
    """Show the details of a workflow."""
    return client.dependency_graph.get_plan(name_or_id)


def show_workflow_command():
    """Command that the details of a workflow."""
    return Command().command(_show_workflow).require_migration()


def _create_workflow(client, output_file, revision, paths):
    """Create a workflow description for a file."""
    graph = Graph(client)
    outputs = graph.build(paths=paths, revision=revision)

    workflow = graph.as_workflow(outputs=outputs,)

    if output_file:
        output_file = Path(output_file)

    wf, path = CWLConverter.convert(workflow, client, path=output_file)

    return wf.export_string()


def create_workflow_command():
    """Command that create a workflow description for a file."""
    return Command().command(_create_workflow)
