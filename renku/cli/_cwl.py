# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

from ._echo import progressbar


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
    outputs = process()

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

    # Keep only unchanged files in the output paths.
    tracked_paths = {
        diff.b_path
        for diff in client.git.index.diff(None)
        if diff.change_type in {'A', 'R', 'M', 'T'} and
        diff.b_path in output_paths
    }
    unchanged_paths = output_paths - tracked_paths

    # Fix tracking of unchanged files by removing them first.
    if unchanged_paths:
        client.git.index.remove(
            unchanged_paths, cached=True, r=True, ignore_unmatch=True
        )
        client.git.index.commit('renku: automatic removal of unchanged files')
        client.git.index.add(unchanged_paths)

        click.echo(
            'Unchanged files:\n\n\t{0}'.format(
                '\n\t'.join(
                    click.style(path, fg='yellow') for path in unchanged_paths
                )
            )
        )
