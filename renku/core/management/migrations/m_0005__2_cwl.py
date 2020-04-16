# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Initial migrations."""

import glob
import os
import uuid
from pathlib import Path

from cwlgen import CommandLineTool, parse_cwl
from git import NULL_TREE, Actor
from werkzeug.utils import secure_filename

from renku.core.models.entities import Collection, Entity
from renku.core.models.locals import with_reference
from renku.core.models.provenance.activities import ProcessRun, WorkflowRun
from renku.core.models.workflow.parameters import CommandArgument, \
    CommandInput, CommandOutput, MappedIOStream
from renku.core.models.workflow.run import Run
from renku.version import __version__, version_url


def migrate(client):
    """Migration function."""
    _migrate_old_workflows(client)


def _migrate_old_workflows(client):
    """Migrates old cwl workflows to new jsonld format."""
    wf_path = '{}/*.cwl'.format(client.workflow_path)

    cwl_paths = glob.glob(wf_path)

    cwl_paths = sorted(
        cwl_paths, key=lambda p: client.find_previous_commit(p).committed_date
    )

    for cwl_file in cwl_paths:
        path = _migrate_cwl(client, cwl_file)
        os.remove(cwl_file)

        client.repo.git.add(cwl_file, path)

        if client.repo.is_dirty():
            commit_msg = ('renku migrate: ' 'committing migrated workflow')

            committer = Actor('renku {0}'.format(__version__), version_url)

            client.repo.index.commit(
                commit_msg,
                committer=committer,
                skip_hooks=True,
            )


def _migrate_cwl(client, path):
    """Migrate a cwl file."""
    workflow = parse_cwl(path)

    if isinstance(workflow, CommandLineTool):
        _, path = _migrate_single_step(client, workflow, path, persist=True)
    else:
        _, path = _migrate_composite_step(client, workflow)

    return path


def _migrate_single_step(client, cmd_line_tool, path, persist=False):
    """Migrate a single step workflow."""
    commit = client.find_previous_commit(path)

    run = Run()
    run.command = ' '.join(cmd_line_tool.baseCommand)
    run.successcodes = cmd_line_tool.successCodes

    inputs = list(cmd_line_tool.inputs)
    outputs = list(cmd_line_tool.outputs)

    if cmd_line_tool.stdin:
        name = cmd_line_tool.stdin.split('.')[1]
        matched_input = next(i for i in inputs if i.id == name)
        inputs.remove(matched_input)

        path = client.workflow_path / Path(matched_input.default['path'])
        stdin = path.resolve().relative_to(client.path)

        run.inputs.append(
            CommandInput(
                consumes=_entity_from_path(client, stdin, commit),
                mapped_to=MappedIOStream(stream_type='stdin')
            )
        )

    if cmd_line_tool.stdout:
        run.outputs.append(
            CommandOutput(
                produces=_entity_from_path(
                    client, cmd_line_tool.stdout, commit
                ),
                mapped_to=MappedIOStream(stream_type='stdout')
            )
        )

        matched_output = next(o for o in outputs if o.id == 'output_stdout')

        if matched_output:
            outputs.remove(matched_output)

    if cmd_line_tool.stderr:
        run.outputs.append(
            CommandOutput(
                produces=_entity_from_path(
                    client, cmd_line_tool.stderr, commit
                ),
                mapped_to=MappedIOStream(stream_type='stderr')
            )
        )

        matched_output = next(o for o in outputs if o.id == 'output_stderr')

        if matched_output:
            outputs.remove(matched_output)

    for o in outputs:
        if not o.outputBinding.glob.startswith('$(inputs.'):
            raise NotImplementedError(
                'Only stdout and outputs mapped to inputs are supported.'
            )
        name = o.outputBinding.glob.split('.')[1]
        matched_input = next(i for i in inputs if i.id == name)
        inputs.remove(matched_input)

        path = client.workflow_path / Path(matched_input.default['path'])
        path = path.relative_to(client.path)

        prefix = matched_input.inputBinding.prefix

        if prefix and matched_input.inputBinding.separate:
            prefix += ' '

        run.outputs.append(
            CommandOutput(
                position=matched_input.inputBinding.position,
                prefix=prefix,
                produces=_entity_from_path(client, path, commit)
            )
        )

    for i in inputs:
        prefix = i.inputBinding.prefix

        if prefix and i.inputBinding.separate:
            prefix += ' '

        if (
            isinstance(i.default, dict) and 'class' in i.default and
            i.default['class'] in ['File', 'Directory']
        ):
            path = client.workflow_path / Path(i.default['path'])
            path = path.resolve().relative_to(client.path)

            run.inputs.append(
                CommandInput(
                    position=i.inputBinding.position,
                    prefix=prefix,
                    consumes=_entity_from_path(client, path, commit)
                )
            )
        else:
            run.arguments.append(
                CommandArgument(
                    position=i.inputBinding.position,
                    prefix=prefix,
                    value=str(i.default)
                )
            )

    for a in cmd_line_tool.arguments:
        run.arguments.append(
            CommandArgument(position=a['position'], value=a['valueFrom'])
        )

    if not persist:
        return run

    step_name = '{0}_{1}.yaml'.format(
        uuid.uuid4().hex,
        secure_filename('_'.join(cmd_line_tool.baseCommand)),
    )

    path = (client.workflow_path / step_name).relative_to(client.path)

    with with_reference(path):
        run.path = path
        process_run = ProcessRun.from_run(run, client, path)
        process_run.invalidated = _invalidations_from_commit(client, commit)
        process_run.to_yaml()
        client.add_to_path_activity_cache(process_run)
        return process_run, path


def _migrate_composite_step(client, workflow):
    """Migrate a composite workflow."""
    run = Run()

    name = '{0}_migrated.yaml'.format(uuid.uuid4().hex)

    run.path = (client.workflow_path / name).relative_to(client.path)

    for step in workflow.steps:
        path = client.workflow_path / step.run
        subrun = workflow = parse_cwl(path)

        subprocess, _ = _migrate_single_step(client, subrun, path)
        subprocess.path = run.path
        run.add_subprocess(subprocess)

    with with_reference(path):
        wf = WorkflowRun.from_run(run, client, path)
        wf.to_yaml()
        client.add_to_path_activity_cache(wf)

    return wf, path


def _entity_from_path(client, path, commit):
    """Gets the entity associated with a path."""
    client, commit, path = client.resolve_in_submodules(
        client.find_previous_commit(path, revision=commit.hexsha),
        path,
    )

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, client.DATASETS)):
        return client.load_dataset_from_path(path, commit=commit)
    else:
        return entity_cls(
            commit=commit,
            client=client,
            path=str(path),
        )


def _invalidations_from_commit(client, commit):
    """Gets invalidated files from a commit."""
    results = []
    collections = dict()
    for file_ in commit.diff(commit.parents or NULL_TREE):
        # only process deleted files (note they appear as ADDED)
        # in this backwards diff
        if file_.change_type != 'A':
            continue
        path_ = Path(file_.a_path)
        entity = _get_activity_entity(
            client, commit, path_, collections, deleted=True
        )

        results.append(entity)

    return results


def _get_activity_entity(client, commit, path, collections, deleted=False):
    """Gets the entity associated with this Activity and path."""
    client, commit, path = client.resolve_in_submodules(
        commit,
        path,
    )
    output_path = client.path / path
    parents = list(output_path.relative_to(client.path).parents)

    collection = None
    members = []
    for parent in reversed(parents[:-1]):
        if str(parent) in collections:
            collection = collections[str(parent)]
        else:
            collection = Collection(
                client=client,
                commit=commit,
                path=str(parent),
                members=[],
                parent=collection,
            )
            members.append(collection)
            collections[str(parent)] = collection

        members = collection.members

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    # TODO: use a factory method to generate the entity
    if str(path).startswith(
        os.path.join(client.renku_home, client.DATASETS)
    ) and not deleted:
        entity = client.load_dataset_from_path(path, commit=commit)
    else:
        entity = entity_cls(
            commit=commit,
            client=client,
            path=str(path),
            parent=collection,
        )

    if collection:
        collection.members.append(entity)

    return entity
