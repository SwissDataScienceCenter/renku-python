# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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

import subprocess
from pathlib import Path

import click

from renku.core.errors import WorkflowRerunError
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject

from .echo import progressbar


def execute_cwl(job, cwl_file, cwl_filename, output_paths):
    """toil.Job function for execute a provided cwl."""
    tempDir = Path(job.fileStore.getLocalTempDir())
    cwl = job.fileStore.readGlobalFile(cwl_file, userPath=str(tempDir / cwl_filename))

    subprocess.check_call(["toil-cwl-runner", cwl])

    outputs = []
    for output in output_paths:
        output_file = job.fileStore.writeGlobalFile(output)
        job.fileStore.readGlobalFile(output_file, userPath=str(tempDir / output))
        outputs.append((output_file, output))
    return outputs


@inject.autoparams()
def execute(output_file, client: LocalClient, output_paths=None):
    """Run the generated workflow using toil-cwl-runner library."""
    output_paths = output_paths or set()

    from toil.common import Toil
    from toil.job import Job
    from toil.leader import FailedJobsException

    options = Job.Runner.getDefaultOptions("./workflow_run")
    options.logLevel = "OFF"
    options.clean = "always"

    try:
        with Toil(options) as toil:
            output_file = Path(output_file)
            cwl_filename = output_file.name
            cwl_file = toil.importFile(output_file.as_uri())
            job = Job.wrapJobFn(execute_cwl, cwl_file, cwl_filename, output_paths)
            locations = toil.start(job)

            with progressbar(length=len(locations), label="Moving outputs") as bar:
                for location in locations:
                    toil.exportFile(location[0], (client.path / location[1]).absolute().as_uri())
                    bar.update(1)
    except FailedJobsException:
        raise WorkflowRerunError(output_file)

    unchanged_paths = client.remove_unmodified(output_paths)
    if unchanged_paths:
        click.echo(
            "Unchanged files:\n\n\t{0}".format("\n\t".join(click.style(path, fg="yellow") for path in unchanged_paths))
        )
