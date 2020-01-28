# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Wrap CWL runner."""

import os
import shutil
import sys

import click

from renku.core.errors import WorkflowRerunError

from .echo import progressbar


def execute(client, output_file, output_paths=None):
    """Run the generated workflow using cwltool library."""
    output_paths = output_paths or set()

    import cwltool.factory
    from cwltool import workflow
    from cwltool.context import LoadingContext, RuntimeContext
    from cwltool.utils import visit_class

    def construct_tool_object(toolpath_object, *args, **kwargs):
        """Fix missing locations."""
        protocol = 'file://'

        def addLocation(d):
            if 'location' not in d and 'path' in d:
                d['location'] = protocol + d['path']

        visit_class(toolpath_object, ('File', 'Directory'), addLocation)
        return workflow.default_make_tool(toolpath_object, *args, **kwargs)

    argv = sys.argv
    sys.argv = ['cwltool']

    # Keep all environment variables.
    runtime_context = RuntimeContext(
        kwargs={
            'rm_tmpdir': False,
            'move_outputs': 'leave',
            'preserve_entire_environment': True,
        }
    )
    loading_context = LoadingContext(
        kwargs={
            'construct_tool_object': construct_tool_object,
        }
    )

    factory = cwltool.factory.Factory(
        loading_context=loading_context,
        runtime_context=runtime_context,
    )
    process = factory.make(os.path.relpath(str(output_file)))
    try:
        outputs = process()
    except cwltool.factory.WorkflowStatus:
        raise WorkflowRerunError(output_file)

    sys.argv = argv

    # Move outputs to correct location in the repository.
    output_dirs = process.factory.executor.output_dirs

    def remove_prefix(location, prefix='file://'):
        if location.startswith(prefix):
            return location[len(prefix):]
        return location

    locations = {
        remove_prefix(output['location'])
        for output in outputs.values()
    }

    with progressbar(
        locations,
        label='Moving outputs',
    ) as bar:
        for location in bar:
            for output_dir in output_dirs:
                if location.startswith(output_dir):
                    output_path = location[len(output_dir):].lstrip(
                        os.path.sep
                    )
                    destination = client.path / output_path
                    if destination.is_dir():
                        shutil.rmtree(str(destination))
                        destination = destination.parent
                    shutil.move(location, str(destination))
                    continue

    unchanged_paths = client.remove_unmodified(output_paths)
    if unchanged_paths:
        click.echo(
            'Unchanged files:\n\n\t{0}'.format(
                '\n\t'.join(
                    click.style(path, fg='yellow') for path in unchanged_paths
                )
            )
        )
