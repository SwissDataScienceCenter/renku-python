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
import traceback
import uuid
from collections import defaultdict
from functools import cmp_to_key
from hashlib import sha1
from pathlib import Path
from urllib.parse import urlparse

from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_0 import CommandLineTool, InitialWorkDirRequirement
from werkzeug.utils import secure_filename

from renku.core import errors
from renku.core.management.migrations.models.v3 import Dataset
from renku.core.management.migrations.models.v9 import (
    Collection,
    CommandArgument,
    CommandInput,
    CommandOutput,
    Entity,
    MappedIOStream,
    Person,
    ProcessRun,
    Run,
    SoftwareAgent,
    WorkflowRun,
)
from renku.core.management.migrations.utils import OLD_DATASETS_PATH, OLD_WORKFLOW_PATH, MigrationType
from renku.core.metadata.repository import Actor, Commit, git_unicode_unescape
from renku.core.utils import communication
from renku.version import __version__, version_url

default_missing_software_agent = SoftwareAgent(
    label="renku pre 0.11.0", id="https://github.com/swissdatasciencecenter/renku-python/tree/pre-0.11.0"
)


def migrate(migration_context):
    """Migration function."""
    if MigrationType.WORKFLOWS not in migration_context.options.type:
        return
    _migrate_old_workflows(client=migration_context.client, strict=migration_context.options.strict)


def _migrate_old_workflows(client, strict):
    """Migrates old cwl workflows to new jsonld format."""

    def sort_cwl_commits(e1, e2):
        """Sorts cwl commits in order of their creation."""
        commit1 = e1[1]
        commit2 = e2[1]

        return commit1.compare_to(commit2)

    cache = RepositoryCache.from_client(client)
    client.cache = cache

    wf_path = f"{client.renku_path}/workflow/*.cwl"
    for path in glob.glob(wf_path):
        if path not in cache.cwl_files_commits:
            raise ValueError(f"Couldn't find a previous commit for path `{path}`")

    cwl_paths = list(cache.cwl_files_commits.items())
    cwl_paths = sorted(cwl_paths, key=cmp_to_key(sort_cwl_commits))

    for n, element in enumerate(cwl_paths, start=1):
        communication.echo(f"Processing commit {n}/{len(cwl_paths)}", end="\r")

        cwl_file, commit = element

        try:
            if not Path(cwl_file).exists():
                continue

            path = _migrate_cwl(client, cwl_file, commit)
            os.remove(cwl_file)

            client.repository.add(cwl_file, path)

            if client.repository.is_dirty():
                commit_msg = "renku migrate: committing migrated workflow"
                committer = Actor(name=f"renku {__version__}", email=version_url)
                client.repository.commit(commit_msg + client.transaction_id, committer=committer, no_verify=True)
        except Exception:
            if strict:
                raise
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Exception: {traceback.format_exc()}")


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
        commit = client.cache.find_previous_commit(path, revision=parent_commit if parent_commit else "HEAD")

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

        matched_input = next(i for i in inputs if i.id.endswith(name))
        inputs.remove(matched_input)

        path = client.renku_path / OLD_WORKFLOW_PATH / Path(matched_input.default["path"])
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

        matched_output = next(o for o in outputs if o.id.endswith("output_stdout"))

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

        matched_output = next(o for o in outputs if o.id.endswith("output_stderr"))

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

            matched_input = next(i for i in all_inputs if i.id.endswith(name))
            try:
                inputs.remove(matched_input)
            except ValueError:
                pass

            if isinstance(matched_input.default, dict):
                path = client.renku_path / OLD_WORKFLOW_PATH / Path(matched_input.default["path"])
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
            path = client.renku_path / OLD_WORKFLOW_PATH / Path(i.default["path"])
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
        id_ = CommandArgument.generate_id(base_id, a.position)
        run.arguments.append(CommandArgument(id=id_, position=a.position, value=a.valueFrom))

    if not persist:
        return run, None

    step_name = "{0}_{1}.yaml".format(uuid.uuid4().hex, secure_filename("_".join(cmd_line_tool.baseCommand)))

    absolute_path = client.renku_path / OLD_WORKFLOW_PATH / step_name
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
    return process_run, absolute_path


def _migrate_composite_step(client, workflow, path, commit=None):
    """Migrate a composite workflow."""
    if not commit:
        commit = client.cache.find_previous_commit(path)
    run = Run(client=client, path=path, commit=commit)
    rel_path = Path(path).relative_to(client.path)
    label = f"{rel_path}@{commit.hexsha}"
    identifier = sha1(label.encode("utf-8")).hexdigest()
    run._id = Run.generate_id(client, identifier=identifier)

    name = "{0}_migrated.yaml".format(uuid.uuid4().hex)

    wf_path = client.renku_path / OLD_WORKFLOW_PATH
    run.path = (wf_path / name).relative_to(client.path)

    for step in workflow.steps:
        if isinstance(step.run, dict):
            continue
        else:
            uri = urlparse(step.run)
            path = uri.path
            subrun = parse_cwl_cached(path)

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

    return wf, run.path


def _entity_from_path(client, path, commit):
    """Gets the entity associated with a path."""
    client, commit, path = client.get_in_submodules(client.cache.find_previous_commit(path, revision=commit), path)

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, OLD_DATASETS_PATH)):
        return Dataset.from_yaml(path=client.path / path, client=client, commit=commit)
    else:
        return entity_cls(commit=commit, client=client, path=str(path))


def _invalidations_from_commit(client, commit: Commit):
    """Gets invalidated files from a commit."""
    results = []
    collections = dict()
    for file in commit.get_changes():
        # NOTE: only process deleted files
        if not file.deleted:
            continue
        path = Path(file.a_path)
        entity = _get_activity_entity(client, commit, path, collections, deleted=True)

        results.append(entity)

    return results


def _get_activity_entity(client, commit, path, collections, deleted=False):
    """Gets the entity associated with this Activity and path."""
    client, commit, path = client.get_in_submodules(commit, path)
    output_path = client.path / path
    parents = list(output_path.relative_to(client.path).parents)

    collection = None
    members = []
    for parent in reversed(parents[:-1]):
        if str(parent) in collections:
            collection = collections[str(parent)]
        else:
            collection = Collection(client=client, commit=commit, path=str(parent), members=[], parent=collection)
            members.append(collection)
            collections[str(parent)] = collection

        members = collection.members

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, OLD_DATASETS_PATH)) and not deleted:
        entity = Dataset.from_yaml(path=client.path / path, client=client, commit=commit)
    else:
        entity = entity_cls(commit=commit, client=client, path=str(path), parent=collection)

    if collection:
        collection.members.append(entity)

    return entity


_cwl_cache = {}


def parse_cwl_cached(path):
    """Parse cwl and remember the result for future execution."""
    if path in _cwl_cache:
        return _cwl_cache[path]

    cwl = load_document_by_uri(path)

    _cwl_cache[path] = cwl

    return cwl


class RepositoryCache:
    """Cache for a git repository."""

    def __init__(self, client, cache, cwl_files_commits):
        self.client = client
        self.cache = cache
        self.cwl_files_commits = cwl_files_commits

    @classmethod
    def from_client(cls, client):
        """Return a cached repository."""
        cache = defaultdict(list)
        cwl_files_commits_map = {}

        for n, commit in enumerate(client.repository.iterate_commits(full_history=True), start=1):
            communication.echo(f"Caching commit {n}", end="\r")

            cwl_files = []
            for file in commit.get_changes():
                # Ignore deleted files
                if file.deleted:
                    continue

                path = file.b_path
                cache[path].append(commit)

                if path.startswith(f"{client.renku_home}/workflow/") and path.endswith(".cwl"):
                    cwl_files.append(os.path.realpath(client.path / path))

            cls._update_cwl_files_and_commits(commit, cwl_files_commits_map, cwl_files)

        communication.echo(40 * " ", end="\r")

        return RepositoryCache(client, cache, cwl_files_commits_map)

    @staticmethod
    def _update_cwl_files_and_commits(commit, cwl_files_commits_map, cwl_files):
        if len(cwl_files) != 1:
            return

        path = cwl_files[0]
        existing_commit = cwl_files_commits_map.get(path)

        if existing_commit is None:
            cwl_files_commits_map[path] = commit
        elif existing_commit.compare_to(commit) < 0:  # existing commit is older
            cwl_files_commits_map[path] = commit

    def find_previous_commit(self, path, revision="HEAD"):
        """Return a previous commit for a given path starting from 'revision'."""

        def find_from_client(path, revision):
            try:
                return self.client.repository.get_previous_commit(path=path, revision=revision, full_history=True)
            except errors.GitCommitNotFoundError:
                communication.warn(f"Cannot find previous commit for {path} from {str(revision)}")
                return revision

        try:
            path = (self.client.path / path).relative_to(self.client.path)
        except ValueError:
            pass
        path = str(path)

        if revision == "HEAD":
            revision = self.client.repository.head.commit

        commits = self.cache.get(git_unicode_unescape(path))
        if not commits:
            return find_from_client(path, revision)

        if revision in commits:
            return revision

        for commit in commits:
            if commit.compare_to(revision) <= 0:
                return commit

        # No commit was found
        return find_from_client(path, revision)
