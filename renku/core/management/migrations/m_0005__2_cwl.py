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
from functools import cmp_to_key
from hashlib import sha1
from pathlib import Path

from cwlgen import CommandLineTool, parse_cwl
from cwlgen.requirements import InitialWorkDirRequirement
from git import NULL_TREE, Actor
from werkzeug.utils import secure_filename

from renku.core.management.migrations.models.v3 import Dataset
from renku.core.models.entities import Collection, Entity
from renku.core.models.provenance.activities import ProcessRun, WorkflowRun
from renku.core.models.provenance.agents import Person, SoftwareAgent
from renku.core.models.workflow.parameters import CommandArgument, CommandInput, CommandOutput, MappedIOStream
from renku.core.models.workflow.run import Run
from renku.core.utils import communication
from renku.core.utils.scm import git_unicode_unescape
from renku.version import __version__, version_url

default_missing_software_agent = SoftwareAgent(
    label="renku pre 0.11.0", id="https://github.com/swissdatasciencecenter/renku-python/tree/pre-0.11.0"
)


def migrate(client):
    """Migration function."""
    _migrate_old_workflows(client)


def _migrate_old_workflows(client):
    """Migrates old cwl workflows to new jsonld format."""

    def sort_cwl_commits(e1, e2):
        """Sorts cwl commits in order of their creation."""
        commit1 = e1[1]
        commit2 = e2[1]

        return _compare_commits(client, commit1, commit2)

    paths_commits_map = _find_cwl_files_and_commits(client)

    wf_path = f"{client.workflow_path}/*.cwl"
    for path in glob.glob(wf_path):
        if path not in paths_commits_map:
            raise ValueError(f"Couldn't find a previous commit for path `{path}`")

    cwl_paths = list(paths_commits_map.items())
    cwl_paths = sorted(cwl_paths, key=cmp_to_key(sort_cwl_commits))

    for n, element in enumerate(cwl_paths, start=1):
        communication.echo(f"Processing commit {n}/{len(cwl_paths)}", end="\r")

        cwl_file, commit = element
        path = _migrate_cwl(client, cwl_file, commit)
        os.remove(cwl_file)

        client.repo.git.add(cwl_file, path)

        if client.repo.is_dirty():
            commit_msg = "renku migrate: " "committing migrated workflow"

            committer = Actor("renku {0}".format(__version__), version_url)

            client.repo.index.commit(
                commit_msg, committer=committer, skip_hooks=True,
            )


def _migrate_cwl(client, path, commit):
    """Migrate a cwl file."""
    workflow = parse_cwl_cached(str(path))

    if isinstance(workflow, CommandLineTool):
        _, path = _migrate_single_step(client, workflow, path, commit=commit, persist=True)
    else:
        _, path = _migrate_composite_step(client, workflow, path, commit=commit)

    return path


def _migrate_single_step(client, cmd_line_tool, path, commit=None, parent_commit=None, persist=False):
    """Migrate a single step workflow."""
    if not commit:
        commit = client.find_previous_commit(path, revision=parent_commit if parent_commit else "HEAD")

    run = Run(client=client, path=path, commit=commit)
    run.command = " ".join(cmd_line_tool.baseCommand)
    run.successcodes = cmd_line_tool.successCodes

    inputs = list(cmd_line_tool.inputs)
    outputs = list(cmd_line_tool.outputs)

    # NOTE: Make run ids deterministic to prevent duplication.
    rel_path = Path(path).relative_to(client.path)
    if parent_commit:
        label = f"{rel_path}@{parent_commit.hexsha}"
    else:
        label = f"{rel_path}@{commit.hexsha}"
    identifier = sha1(label.encode("utf-8")).hexdigest()

    base_id = Run.generate_id(client, identifier=identifier)
    run._id = base_id

    if cmd_line_tool.stdin:
        name = cmd_line_tool.stdin.split(".")[1]

        if name.endswith(")"):
            name = name[:-1]

        matched_input = next(i for i in inputs if i.id == name)
        inputs.remove(matched_input)

        path = client.workflow_path / Path(matched_input.default["path"])
        stdin = path.resolve().relative_to(client.path)
        id_ = CommandInput.generate_id(base_id, "stdin")

        run.inputs.append(
            CommandInput(
                id=id_,
                consumes=_entity_from_path(client, stdin, commit),
                mapped_to=MappedIOStream(client=client, stream_type="stdin"),
            )
        )

    if cmd_line_tool.stdout:
        run.outputs.append(
            CommandOutput(
                id=CommandOutput.generate_id(base_id, "stdout"),
                produces=_entity_from_path(client, cmd_line_tool.stdout, commit),
                mapped_to=MappedIOStream(client=client, stream_type="stdout"),
                create_folder=False,
            )
        )

        matched_output = next(o for o in outputs if o.id == "output_stdout")

        if matched_output:
            outputs.remove(matched_output)

    if cmd_line_tool.stderr:
        run.outputs.append(
            CommandOutput(
                id=CommandOutput.generate_id(base_id, "stderr"),
                produces=_entity_from_path(client, cmd_line_tool.stderr, commit),
                mapped_to=MappedIOStream(client=client, stream_type="stderr"),
                create_folder=False,
            )
        )

        matched_output = next(o for o in outputs if o.id == "output_stderr")

        if matched_output:
            outputs.remove(matched_output)

    created_outputs = []
    workdir_requirements = [r for r in cmd_line_tool.requirements if isinstance(r, InitialWorkDirRequirement)]

    for r in workdir_requirements:
        for listing in r.listing:
            if listing.entry == '$({"listing": [], "class": "Directory"})':
                created_outputs.append(listing.entryname)

    # NOTE: multiple outputs might bind to the same input; we use this copy to find output bindings
    all_inputs = inputs.copy()

    for o in outputs:
        prefix = None
        position = None

        if o.outputBinding.glob.startswith("$(inputs."):
            name = o.outputBinding.glob.split(".")[1]

            if name.endswith(")"):
                name = name[:-1]

            matched_input = next(i for i in all_inputs if i.id == name)
            try:
                inputs.remove(matched_input)
            except ValueError:
                pass

            if isinstance(matched_input.default, dict):
                path = client.workflow_path / Path(matched_input.default["path"])
            else:
                path = Path(matched_input.default)

            path = Path(os.path.realpath(client.path / path)).relative_to(client.path)

            if matched_input.inputBinding:
                prefix = matched_input.inputBinding.prefix
                position = matched_input.inputBinding.position

                if prefix and matched_input.inputBinding.separate:
                    prefix += " "
        else:
            path = Path(o.outputBinding.glob)

        create_folder = False

        check_path = path
        if not (client.path / path).is_dir():
            check_path = path.parent

        if check_path != "." and str(check_path) in created_outputs:
            create_folder = True

        run.outputs.append(
            CommandOutput(
                id=CommandOutput.generate_id(base_id, position),
                position=position,
                prefix=prefix,
                produces=_entity_from_path(client, path, commit),
                create_folder=create_folder,
            )
        )

    for i in inputs:
        prefix = None
        position = None

        if i.inputBinding:
            prefix = i.inputBinding.prefix
            position = i.inputBinding.position

            if prefix and i.inputBinding.separate:
                prefix += " "

        if isinstance(i.default, dict) and "class" in i.default and i.default["class"] in ["File", "Directory"]:
            path = client.workflow_path / Path(i.default["path"])
            path = Path(os.path.realpath(path)).relative_to(client.path)

            run.inputs.append(
                CommandInput(
                    id=CommandInput.generate_id(base_id, position),
                    position=position,
                    prefix=prefix,
                    consumes=_entity_from_path(client, path, commit),
                )
            )
        else:
            run.arguments.append(
                CommandArgument(
                    id=CommandArgument.generate_id(base_id, position),
                    position=position,
                    prefix=prefix,
                    value=str(i.default),
                )
            )

    for a in cmd_line_tool.arguments:
        id_ = CommandArgument.generate_id(base_id, a["position"])
        run.arguments.append(CommandArgument(id=id_, position=a["position"], value=a["valueFrom"]))

    if not persist:
        return run, None

    step_name = "{0}_{1}.yaml".format(uuid.uuid4().hex, secure_filename("_".join(cmd_line_tool.baseCommand)),)

    absolute_path = client.workflow_path / step_name
    path = absolute_path.relative_to(client.path)

    run.path = path
    process_run = ProcessRun.from_run(run, client, path, commit=commit)
    process_run.invalidated = _invalidations_from_commit(client, commit)

    # HACK: This fixes broken SoftwareAgent due to rebases done by users
    if isinstance(process_run.association.agent, Person) or not process_run.association.agent.label.startswith(
        "renku "
    ):
        process_run.association.agent = default_missing_software_agent
    process_run.to_yaml(path=absolute_path)
    client.add_to_activity_index(process_run)
    return process_run, absolute_path


def _migrate_composite_step(client, workflow, path, commit=None):
    """Migrate a composite workflow."""
    if not commit:
        commit = client.find_previous_commit(path)
    run = Run(client=client, path=path, commit=commit)
    rel_path = Path(path).relative_to(client.path)
    label = f"{rel_path}@{commit.hexsha}"
    identifier = sha1(label.encode("utf-8")).hexdigest()
    run._id = Run.generate_id(client, identifier=identifier)

    name = "{0}_migrated.yaml".format(uuid.uuid4().hex)

    run.path = (client.workflow_path / name).relative_to(client.path)

    for step in workflow.steps:
        if isinstance(step.run, dict):
            continue
        else:
            path = client.workflow_path / step.run
            subrun = parse_cwl_cached(str(path))

        subprocess, _ = _migrate_single_step(client, subrun, path, parent_commit=commit)
        run.add_subprocess(subprocess)

    wf = WorkflowRun.from_run(run, client, run.path, commit=commit)

    # HACK: This fixes broken SoftwareAgent due to rebases done by users
    if isinstance(wf.association.agent, Person) or not wf.association.agent.label.startswith("renku "):
        wf.association.agent = default_missing_software_agent
    for p in wf._processes:
        if isinstance(p.association.agent, Person) or not p.association.agent.label.startswith("renku "):
            p.association.agent = default_missing_software_agent
    wf.to_yaml(run.path)
    client.add_to_activity_index(wf)

    return wf, run.path


def _entity_from_path(client, path, commit):
    """Gets the entity associated with a path."""
    client, commit, path = client.resolve_in_submodules(
        client.find_previous_commit(path, revision=commit.hexsha), path,
    )

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, client.DATASETS)):
        return Dataset.from_yaml(path=client.path / path, client=client, commit=commit)
    else:
        return entity_cls(commit=commit, client=client, path=str(path),)


def _invalidations_from_commit(client, commit):
    """Gets invalidated files from a commit."""
    results = []
    collections = dict()
    for file_ in commit.diff(commit.parents or NULL_TREE):
        # only process deleted files (note they appear as ADDED)
        # in this backwards diff
        if file_.change_type != "A":
            continue
        path_ = Path(git_unicode_unescape(file_.a_path))
        entity = _get_activity_entity(client, commit, path_, collections, deleted=True)

        results.append(entity)

    return results


def _get_activity_entity(client, commit, path, collections, deleted=False):
    """Gets the entity associated with this Activity and path."""
    client, commit, path = client.resolve_in_submodules(commit, path,)
    output_path = client.path / path
    parents = list(output_path.relative_to(client.path).parents)

    collection = None
    members = []
    for parent in reversed(parents[:-1]):
        if str(parent) in collections:
            collection = collections[str(parent)]
        else:
            collection = Collection(client=client, commit=commit, path=str(parent), members=[], parent=collection,)
            members.append(collection)
            collections[str(parent)] = collection

        members = collection.members

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, client.DATASETS)) and not deleted:
        entity = Dataset.from_yaml(path=client.path / path, client=client, commit=commit)
    else:
        entity = entity_cls(commit=commit, client=client, path=str(path), parent=collection,)

    if collection:
        collection.members.append(entity)

    return entity


_cwl_cache = {}


def parse_cwl_cached(path):
    """Parse cwl and remember the result for future execution."""
    if path in _cwl_cache:
        return _cwl_cache[path]

    cwl = parse_cwl(path)

    _cwl_cache[path] = cwl

    return cwl


def _find_cwl_files_and_commits(client):
    """Return a dict of paths and last commit that changed them."""

    def get_cwl_files(commit):
        files = []

        for file in commit.diff(commit.parents or NULL_TREE, paths=f"{client.workflow_path}/*.cwl"):
            # Ignore deleted files (they appear as ADDED in this backwards diff)
            if file.change_type == "A":
                continue
            path = file.a_path
            if not path.startswith(client.cwl_prefix) or not path.endswith(".cwl"):
                continue

            files.append(os.path.realpath(client.path / path))

            if len(files) > 1:  # The commit is ignored if it has more than one CWL file
                break

        return files

    files_commits_map = {}
    wf_paths = f"{client.workflow_path}/*.cwl"
    for n, commit in enumerate(client.repo.iter_commits(paths=wf_paths, full_history=True), start=1):
        communication.echo(f"Collecting CWL files {n}", end="\r")

        files = get_cwl_files(commit)
        if len(files) != 1:
            continue

        path = files[0]
        existing_commit = files_commits_map.get(path)

        if existing_commit is None:
            files_commits_map[path] = commit
        elif _compare_commits(client, existing_commit, commit) < 0:  # existing commit is older
            files_commits_map[path] = commit

    communication.echo(40 * " ", end="\r")

    return files_commits_map


def _compare_commits(client, commit1, commit2):
    """Return -1 if commit1 is made before commit2."""
    if client.repo.is_ancestor(commit1, commit2):
        return -1
    if client.repo.is_ancestor(commit2, commit1):
        return 1

    if commit1.committed_date < commit2.committed_date:
        return -1
    if commit1.committed_date > commit2.committed_date:
        return 1

    if commit1.authored_date < commit2.authored_date:
        return -1
    if commit1.authored_date > commit2.authored_date:
        return 1
    raise ValueError(
        f"Cannot order commits {commit1} and {commit2}, there is no "
        "dependency between them and they have identical commit and "
        "author dates"
    )
