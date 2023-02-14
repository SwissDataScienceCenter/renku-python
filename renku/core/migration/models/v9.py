# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Migration models V9."""

import datetime
import os
import posixpath
import re
import uuid
import weakref
from bisect import bisect
from collections import OrderedDict
from copy import copy
from functools import total_ordering
from pathlib import Path
from typing import Type, Union
from urllib.parse import quote, urljoin, urlparse

import attr
from attr.validators import instance_of
from marshmallow import EXCLUDE, pre_dump

from renku.command.schema.agent import PersonSchema
from renku.command.schema.annotation import AnnotationSchema
from renku.command.schema.calamus import (
    DateTimeList,
    JsonLDSchema,
    Nested,
    StringList,
    Uri,
    fields,
    oa,
    prov,
    rdfs,
    renku,
    schema,
)
from renku.command.schema.project import ProjectSchema as V10ProjectSchema
from renku.core import errors
from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION
from renku.core.migration.models import v10 as new_schema
from renku.core.migration.models.refs import LinkReference
from renku.core.migration.utils import (
    OLD_METADATA_PATH,
    generate_dataset_file_url,
    generate_dataset_id,
    generate_dataset_tag_id,
    generate_url_id,
    get_datasets_path,
)
from renku.core.util import yaml as yaml
from renku.core.util.datetime8601 import fix_datetime, parse_date
from renku.core.util.doi import extract_doi, is_doi
from renku.core.util.git import get_in_submodules
from renku.core.util.urls import get_host, get_slug
from renku.domain_model.dataset import generate_default_name
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Commit
from renku.version import __version__, version_url

wfprov = fields.Namespace("http://purl.org/wf4ever/wfprov#")
PROJECT_URL_PATH = "projects"
RANDOM_ID_LENGTH = 4


def _set_entity_commit(entity, commit):
    """Set the commit of an entity."""
    if not entity.commit:
        revision = "UNCOMMITTED"
        if entity._label:
            revision = entity._label.rsplit("@", maxsplit=1)[-1]
        if revision == "UNCOMMITTED":
            commit = commit
        elif project_context.has_context():
            commit = project_context.repository.get_commit(revision)
        entity.commit = commit


def _str_or_none(data):
    """Return str representation or None."""
    return str(data) if data is not None else data


def generate_project_id(name, creator):
    """Return the id for the project based on the repository origin remote."""

    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"

    if not creator:
        raise ValueError("Project Creator not set")

    owner = creator.email.split("@")[0]

    if project_context.has_context():
        remote = project_context.remote
        host = remote.host or host
        owner = remote.owner or owner
        name = remote.name or name
    host = os.environ.get("RENKU_DOMAIN") or host
    if name:
        name = quote(name, safe="")
    else:
        raise ValueError("Project name not set")

    project_url = urljoin(f"https://{host}", posixpath.join(PROJECT_URL_PATH, owner, name))
    return project_url


@attr.s(slots=True)
class Project:
    """Represent a project."""

    name = attr.ib(default=None)

    created = attr.ib(converter=parse_date)

    version = attr.ib(converter=str, default=str(SUPPORTED_PROJECT_VERSION))

    agent_version = attr.ib(converter=str, default="pre-0.11.0")

    template_source = attr.ib(type=str, default=None)

    template_ref = attr.ib(type=str, default=None)

    template_id = attr.ib(type=str, default=None)

    template_version = attr.ib(type=str, default=None)

    template_metadata = attr.ib(type=str, default="{}")

    immutable_template_files = attr.ib(factory=list)

    automated_update = attr.ib(converter=bool, default=True)

    creator = attr.ib(default=None, kw_only=True)

    _id = attr.ib(kw_only=True, default=None)

    _metadata_path = attr.ib(default=None, init=False)

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        if not self.creator:
            old_metadata_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
            repository = project_context.repository
            if old_metadata_path.exists():
                self.creator = Person.from_commit(repository.get_previous_commit(old_metadata_path, first=True))
            else:
                # this assumes the project is being newly created
                self.creator = Person.from_repository(repository)

        try:
            self._id = self.project_id
        except ValueError:
            """Fallback to old behaviour."""
            if not self._id:
                try:
                    self._id = project_context.project.id
                except ValueError:
                    metadata_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
                    self._id = Project.from_yaml(metadata_path)._id
                except errors.ConfigurationError:
                    pass
                if not self._id:
                    raise

    @property
    def project_id(self):
        """Return the id for the project."""
        return generate_project_id(name=self.name, creator=self.creator)

    @classmethod
    def from_yaml(cls, path):
        """Return an instance from a YAML file."""
        data = yaml.read_yaml(path)
        self = cls.from_jsonld(data=data)
        self._metadata_path = path

        return self

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return ProjectSchema().load(data)

    def to_yaml(self, path=None):
        """Write an instance to the referenced YAML file."""
        from renku import __version__

        self.agent_version = __version__

        self._metadata_path = path or self._metadata_path
        data = ProjectSchema().dump(self)
        yaml.write_yaml(path=self._metadata_path, data=data)


@attr.s(eq=False, order=False)
class CommitMixin:
    """Represent a commit mixin."""

    commit = attr.ib(default=None, kw_only=True)
    path = attr.ib(default=None, kw_only=True, converter=_str_or_none)

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(kw_only=True)
    _project = attr.ib(type=Project, kw_only=True, default=None)

    def default_id(self):
        """Configure calculated ID."""
        hexsha = self.commit.hexsha if self.commit else "UNCOMMITTED"
        return generate_file_id(hexsha=hexsha, path=self.path)

    @_label.default
    def default_label(self):
        """Generate a default label."""
        if self.commit:
            hexsha = self.commit.hexsha
        else:
            hexsha = "UNCOMMITTED"
        if self.path:
            path = self.path
            if project_context.has_context() and os.path.isabs(path):
                path = Path(path).relative_to(project_context.path)
            return generate_label(path, hexsha)
        return hexsha

    def __attrs_post_init__(self):
        """Post-init hook."""
        if self.path and project_context.has_context():
            path = Path(self.path)
            if path.is_absolute():
                self.path = str(path.relative_to(project_context.path))

        # always force "project" to be the current project
        if project_context.has_context():
            try:
                self._project = project_context.project
            except ValueError:
                metadata_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
                self._project = Project.from_yaml(metadata_path)

        if not self._id:
            self._id = self.default_id()


@attr.s(eq=False, order=False)
class Entity(CommitMixin):
    """Represent a data value or item."""

    _parent = attr.ib(
        default=None, kw_only=True, converter=lambda value: weakref.ref(value) if value is not None else None
    )

    checksum = attr.ib(default=None, kw_only=True, type=str)

    @classmethod
    def from_revision(cls, path, revision: Union[str, Commit] = "HEAD", parent=None, find_previous=True, **kwargs):
        """Return dependency from given path and revision."""
        repository = project_context.repository

        if find_previous:
            revision = repository.get_previous_commit(path, revision=revision)
        elif revision == "HEAD":
            revision = repository.head.commit
        else:
            assert isinstance(revision, Commit)

        _, commit, path = get_in_submodules(project_context.repository, revision, path)

        path_ = project_context.path / path
        if path != "." and path_.is_dir():
            entity = Collection(commit=commit, path=path, members=[], parent=parent)

            files_in_commit = [c.b_path for c in commit.get_changes() if not c.deleted]

            # update members with commits
            for member in path_.iterdir():
                if member.name == ".gitkeep":
                    continue

                member_path = str(member.relative_to(project_context.path))
                find_previous = True

                if member_path in files_in_commit:
                    # we already know the newest commit, no need to look it up
                    find_previous = False

                try:
                    assert all(member_path != m.path for m in entity.members)

                    entity.members.append(
                        cls.from_revision(
                            path=member_path,
                            revision=commit,
                            parent=entity,
                            find_previous=find_previous,
                            **kwargs,
                        )
                    )
                except errors.GitCommitNotFoundError:
                    pass

        else:
            entity = cls(commit=commit, path=str(path), parent=parent, **kwargs)

        return entity

    @property
    def parent(self):  # pragma: no cover
        """Return the parent object."""
        return self._parent() if self._parent is not None else None

    @property
    def entities(self):
        """Yield itself."""
        if project_context.has_context() and not self.commit and self._label and "@UNCOMMITTED" not in self._label:
            repository = project_context.repository
            self.commit = repository.get_commit(self._label.rsplit("@", maxsplit=1)[-1])

        yield self


@attr.s(eq=False, order=False)
class Collection(Entity):
    """Represent a directory with files."""

    members = attr.ib(kw_only=True, default=None)

    def __attrs_post_init__(self):
        """Init members."""
        super().__attrs_post_init__()

        if self.members is None:
            self.members = self.default_members()

        for member in self.members:
            member._parent = weakref.ref(self)

    def default_members(self):
        """Generate default members as entities from current path."""
        if not project_context.has_context():
            return []
        dir_path = project_context.path / self.path

        if not dir_path.exists():
            # likely a directory deleted in a previous commit
            return []

        assert dir_path.is_dir()

        members = []
        for path in dir_path.iterdir():
            if path.name == ".gitkeep":
                continue  # ignore empty directories in Git repository
            cls: Type = Collection if path.is_dir() else Entity
            members.append(cls(commit=self.commit, path=str(path.relative_to(project_context.path)), parent=self))
        return members

    @property
    def entities(self):
        """Recursively return all files."""
        for member in self.members:
            yield from member.entities

        if project_context.has_context() and not self.commit and self._label and "@UNCOMMITTED" not in self._label:
            repository = project_context.repository
            self.commit = repository.get_commit(self._label.rsplit("@", maxsplit=1)[-1])

        yield self


@attr.s(eq=False, order=False)
class MappedIOStream(object):
    """Represents an IO stream (``stdin``, ``stdout``, ``stderr``)."""

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    STREAMS = ["stdin", "stdout", "stderr"]

    stream_type = attr.ib(type=str, kw_only=True)

    def default_id(self):
        """Generate an id for a mapped stream."""
        host = "localhost"
        if project_context.has_context():
            host = project_context.remote.host or host
        host = os.environ.get("RENKU_DOMAIN") or host

        return urljoin("https://{host}".format(host=host), posixpath.join("/iostreams", self.stream_type))

    def default_label(self):
        """Set default label."""
        return 'Stream mapping for stream "{}"'.format(self.stream_type)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._id:
            self._id = self.default_id()
        if not self._label:
            self._label = self.default_label()


@attr.s(eq=False, order=False)
class CommandParameter:
    """Represents a parameter for an execution template."""

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    default_value = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    name: str = attr.ib(default=None, kw_only=True)

    position = attr.ib(default=None, type=int, kw_only=True)

    prefix = attr.ib(default=None, type=str, kw_only=True)

    @property
    def sanitized_id(self):
        """Return ``_id`` sanitized for use in non-jsonld contexts."""
        if "/steps/" in self._id:
            return "/".join(self._id.split("/")[-4:])
        return "/".join(self._id.split("/")[-2:])

    def default_label(self):
        """Set default label."""
        raise NotImplementedError

    def default_name(self):
        """Create a default name."""
        raise NotImplementedError

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._label:
            self._label = self.default_label()
        if not self.name:
            self.name = self.default_name()


def _generate_name(base, prefix, position):
    name = get_slug(prefix.strip(" -=")) if prefix else base
    position = position or uuid.uuid4().hex[:RANDOM_ID_LENGTH]
    return f"{name}-{position}"


@attr.s(eq=False, order=False)
class CommandArgument(CommandParameter):
    """An argument to a command that is neither input nor output."""

    value = attr.ib(default=None, type=str, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/arguments/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Argument "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="param", prefix=self.prefix, position=self.position)

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.value


@attr.s(eq=False, order=False)
class CommandInput(CommandParameter):
    """An input to a command."""

    consumes = attr.ib(kw_only=True)

    mapped_to = attr.ib(default=None, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/inputs/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Input "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="input", prefix=self.prefix, position=self.position)

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.consumes.path


@attr.s(eq=False, order=False)
class CommandOutput(CommandParameter):
    """An output of a command."""

    create_folder = attr.ib(default=False, kw_only=True, type=bool)

    produces = attr.ib(kw_only=True)

    mapped_to = attr.ib(default=None, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/outputs/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Output "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="output", prefix=self.prefix, position=self.position)

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.produces.path


@attr.s(eq=False, order=False)
class RunParameter:
    """A run parameter that is set inside the script."""

    _id = attr.ib(default=None, kw_only=True)

    _label = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, type=str, kw_only=True)

    value = attr.ib(default=None, type=str, kw_only=True)

    type = attr.ib(default=None, type=str, kw_only=True)


@total_ordering
@attr.s(eq=False, order=False)
class Run(CommitMixin):
    """Represents a `renku run` execution template."""

    command = attr.ib(default=None, type=str, kw_only=True)

    successcodes = attr.ib(kw_only=True, type=list, factory=list)

    subprocesses = attr.ib(kw_only=True, factory=list)

    arguments = attr.ib(kw_only=True, factory=list)

    inputs = attr.ib(kw_only=True, factory=list)

    outputs = attr.ib(kw_only=True, factory=list)

    run_parameters = attr.ib(kw_only=True, factory=list)

    name = attr.ib(default=None, kw_only=True, type=str)

    description = attr.ib(default=None, kw_only=True, type=str)

    keywords = attr.ib(kw_only=True, factory=list)

    _activity = attr.ib(kw_only=True, default=None)

    @staticmethod
    def generate_id(identifier=None):
        """Generate an id for an argument."""
        host = "localhost"
        host = project_context.remote.host or host
        host = os.environ.get("RENKU_DOMAIN") or host

        if not identifier:
            identifier = str(uuid.uuid4())

        return urljoin("https://{host}".format(host=host), posixpath.join("/runs", quote(identifier, safe="")))

    def __lt__(self, other):
        """Compares two subprocesses order based on their dependencies."""
        a_inputs = set()
        b_outputs = set()

        for i in other.inputs:
            entity = i.consumes
            for sub_entity in entity.entities:
                a_inputs.add(sub_entity.path)

        for i in self.outputs:
            entity = i.produces
            for sub_entity in entity.entities:
                b_outputs.add(sub_entity.path)

        return a_inputs & b_outputs

    def add_subprocess(self, subprocess):
        """Adds a subprocess to this run."""
        process_order = 0
        if self.subprocesses:
            processes = [o.process for o in self.subprocesses]
            # Get position to insert based on dependencies
            process_order = bisect(processes, subprocess)
            if process_order < len(processes):
                # adjust ids of inputs inherited from latter subprocesses
                for i in range(len(processes), process_order, -1):
                    sp = self.subprocesses[i - 1]
                    sp._id = sp._id.replace(f"subprocess/{i}", f"subprocess/{i+1}")
                    sp.index += 1

                    for inp in self.inputs:
                        inp._id = inp._id.replace(f"/steps/step_{i}/", f"/steps/step_{i+1}/")
                    for outp in self.outputs:
                        outp._id = outp._id.replace(f"/steps/step_{i}/", f"/steps/step_{i+1}/")

        input_paths = [i.consumes.path for i in self.inputs]
        output_paths = [o.produces.path for o in self.outputs]

        for input_ in subprocess.inputs:
            if input_.consumes.path not in input_paths and input_.consumes.path not in output_paths:
                new_input = copy(input_)

                new_input._id = f"{self._id}/steps/step_{process_order + 1}/" f"{new_input.sanitized_id}"
                new_input.mapped_to = None

                matching_output = next((o for o in self.outputs if o.produces.path == new_input.consumes.path), None)

                if not matching_output:
                    self.inputs.append(new_input)
                    input_paths.append(new_input.consumes.path)

        for output in subprocess.outputs:
            if output.produces.path not in output_paths:
                new_output = copy(output)

                new_output._id = f"{self._id}/steps/step_{process_order + 1}/" f"{new_output.sanitized_id}"
                new_output.mapped_to = None
                self.outputs.append(new_output)
                output_paths.append(new_output.produces.path)

                matching_input = next((i for i in self.inputs if i.consumes.path == new_output.produces.path), None)
                if matching_input:
                    self.inputs.remove(matching_input)
                    input_paths.remove(matching_input.consumes.path)
        ordered_process = OrderedSubprocess(
            id=OrderedSubprocess.generate_id(self._id, process_order + 1), index=process_order + 1, process=subprocess
        )
        self.subprocesses.insert(process_order, ordered_process)


@total_ordering
@attr.s(eq=False, order=False)
class OrderedSubprocess:
    """A subprocess with ordering."""

    _id = attr.ib(kw_only=True)

    index = attr.ib(kw_only=True, type=int)

    process = attr.ib(kw_only=True)

    @staticmethod
    def generate_id(parent_id, index):
        """Generate an id for an ``OrderedSubprocess``."""
        return f"{parent_id}/subprocess/{index}"

    def __lt__(self, other):
        """Compares two ordered subprocesses."""
        return self.index < other.index


@attr.s
class Association:
    """Assign responsibility to an agent for an activity."""

    plan = attr.ib()
    agent = attr.ib(default=None)

    _id = attr.ib(kw_only=True)


class EntityProxyMixin:
    """Implement proxy to entity attribute."""

    def __getattribute__(self, name):
        """Proxy entity attributes."""
        cls = object.__getattribute__(self, "__class__")
        names = {field.name for field in attr.fields(cls)}
        names |= set(dir(cls))
        if name in names:
            return object.__getattribute__(self, name)
        entity = object.__getattribute__(self, "entity")
        return getattr(entity, name)


@attr.s(eq=False, order=False)
class Usage(EntityProxyMixin):
    """Represent a dependent path."""

    entity = attr.ib(kw_only=True)
    role = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)


@attr.s(eq=False, order=False)
class Generation(EntityProxyMixin):
    """Represent an act of generating a file."""

    entity = attr.ib()

    role = attr.ib(default=None)

    _activity = attr.ib(
        default=None, kw_only=True, converter=lambda value: weakref.ref(value) if value is not None else None
    )
    _id = attr.ib(kw_only=True)

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity() if self._activity is not None else None

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        if self.role:
            return f"{self.activity._id}/{self.role}"
        return f"{self.activity._id}/tree/{quote(str(self.entity.path))}"


@attr.s(eq=False, order=False)
class Activity(CommitMixin):
    """Represent an activity in the repository."""

    _id = attr.ib(default=None, kw_only=True)
    _message = attr.ib(kw_only=True)
    _was_informed_by = attr.ib(kw_only=True)

    part_of = attr.ib(default=None, kw_only=True)

    _collections = attr.ib(default=attr.Factory(OrderedDict), init=False, kw_only=True)
    generated = attr.ib(kw_only=True, default=None)

    invalidated = attr.ib(kw_only=True, default=None)

    influenced = attr.ib(kw_only=True)

    started_at_time = attr.ib(kw_only=True)

    ended_at_time = attr.ib(kw_only=True)

    agents = attr.ib(kw_only=True)

    _metadata_path = attr.ib(default=None, init=False)

    @classmethod
    def from_yaml(cls, path, commit=None):
        """Return an instance from a YAML file."""
        data = yaml.read_yaml(path)

        self = cls.from_jsonld(data=data, commit=commit)
        self._metadata_path = path

        return self

    @classmethod
    def from_jsonld(cls, data, commit=None):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, list):
            raise ValueError(data)

        schema = ActivitySchema

        if any(str(wfprov.WorkflowRun) in d["@type"] for d in data):
            schema = WorkflowRunSchema
        elif any(str(wfprov.ProcessRun) in d["@type"] for d in data):
            schema = ProcessRunSchema

        return schema(commit=commit, flattened=True).load(data)

    @_message.default
    def default_message(self):
        """Generate a default message."""
        if self.commit:
            return self.commit.message

    @_was_informed_by.default
    def default_was_informed_by(self):
        """List parent actions."""
        if self.commit:
            return [self.generate_id(parent) for parent in self.commit.parents]

    @started_at_time.default
    def default_started_at_time(self):
        """Configure calculated properties."""
        if self.commit:
            return self.commit.authored_datetime

    @ended_at_time.default
    def default_ended_at_time(self):
        """Configure calculated properties."""
        if self.commit:
            return self.commit.committed_datetime

    @agents.default
    def default_agents(self):
        """Set person agent to be the author of the commit."""
        renku_agent = SoftwareAgent(label="renku {0}".format(__version__), id=version_url)
        if self.commit:
            return [Person.from_commit(self.commit), renku_agent]
        return [renku_agent]

    @influenced.default
    def default_influenced(self):
        """Calculate default values."""
        return list(self._collections.values())


@attr.s(eq=False, order=False)
class ProcessRun(Activity):
    """A process run is a particular execution of a Process description."""

    __association_cls__ = Run

    generated = attr.ib(kw_only=True, default=None)

    association = attr.ib(default=None, kw_only=True)

    annotations = attr.ib(kw_only=True, default=None)

    qualified_usage = attr.ib(kw_only=True, default=None)

    run_parameter = attr.ib(kw_only=True, default=None)

    def __attrs_post_init__(self):
        """Calculate properties."""
        super().__attrs_post_init__()
        repository = project_context.repository
        commit_not_set = not self.commit or self.commit.hexsha in self._id
        if commit_not_set and Path(self.path).exists():
            self.commit = repository.get_previous_commit(self.path)

        if self.association:
            self.association.plan._activity = weakref.ref(self)
            plan = self.association.plan
            if not plan.commit:
                if self.commit:
                    plan.commit = self.commit

                if plan.inputs:
                    for i in plan.inputs:
                        _set_entity_commit(entity=i.consumes, commit=self.commit)
                if plan.outputs:
                    for o in plan.outputs:
                        _set_entity_commit(entity=o.produces, commit=self.commit)

        if self.qualified_usage and self.commit:
            usages = []
            revision = self.commit.hexsha
            for usage in self.qualified_usage:
                if not usage.commit and "@UNCOMMITTED" in usage._label:
                    usages.append(
                        Usage.from_revision(path=usage.path, role=usage.role, revision=revision, id=usage._id)
                    )
                else:
                    if not usage.commit:
                        revision = usage._label.rsplit("@", maxsplit=1)[-1]
                        usage.entity.commit = repository.get_commit(revision)

                    usages.append(usage)
            self.qualified_usage = usages

    @classmethod
    def generate_id(cls, commit_hexsha):
        """Calculate action ID."""
        host = "localhost"
        host = project_context.remote.host or host
        host = os.environ.get("RENKU_DOMAIN") or host

        return urljoin(
            "https://{host}".format(host=host),
            posixpath.join("/activities", f"commit/{commit_hexsha}"),
        )

    @classmethod
    def from_run(cls, run, path, commit=None, subprocess_index=None, update_commits=False):
        """Convert a ``Run`` to a ``ProcessRun``."""
        repository = project_context.repository

        if not commit:
            commit = repository.head.commit

        usages = []

        id_ = ProcessRun.generate_id(commit.hexsha)

        if subprocess_index is not None:
            id_ = f"{id_}/steps/step_{subprocess_index}"

        for input_ in run.inputs:
            usage_id = f"{id_}/{input_.sanitized_id}"
            input_path = input_.consumes.path
            entity = input_.consumes
            if update_commits:
                commit = repository.get_previous_commit(input_path, revision=commit.hexsha)
                entity = Entity.from_revision(input_path, commit)

            dependency = Usage(entity=entity, role=input_.sanitized_id, id=usage_id)

            usages.append(dependency)

        agent = SoftwareAgent.from_commit(commit)
        association = Association(agent=agent, id=id_ + "/association", plan=run)

        run_parameter = []

        for parameter in run.run_parameters:
            parameter_id = f"{id_}/{parameter.name}"
            run_parameter.append(RunParameter(name=parameter.name, value=parameter.value, id=parameter_id))

        process_run = cls(
            id=id_,
            qualified_usage=usages,
            association=association,
            commit=commit,
            path=path,
            run_parameter=run_parameter,
        )

        generated = []

        for output in run.outputs:
            entity = Entity.from_revision(output.produces.path, revision=commit, parent=output.produces.parent)

            generation = Generation(activity=process_run, role=output.sanitized_id, entity=entity)
            generated.append(generation)

        process_run.generated = generated

        return process_run

    def to_yaml(self, path=None):
        """Write an instance to the referenced YAML file."""
        self._metadata_path = path or self._metadata_path
        data = ProcessRunSchema(flattened=True).dump(self)
        yaml.write_yaml(path=self._metadata_path, data=data)


@attr.s(eq=False, order=False)
class WorkflowRun(ProcessRun):
    """A workflow run typically contains several subprocesses."""

    __association_cls__ = Run

    _processes = attr.ib(kw_only=True, default=attr.Factory(list))

    @property
    def subprocesses(self):
        """Subprocesses of this ``WorkflowRun``."""
        return {i: p for i, p in enumerate(self._processes)}


@attr.s
class Url:
    """Represents a schema URL reference."""

    url = attr.ib(default=None, kw_only=True)

    url_str = attr.ib(default=None, kw_only=True)
    url_id = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)

    def default_id(self):
        """Define default value for id field."""
        return generate_url_id(url_str=self.url_str, url_id=self.url_id)

    def default_url(self):
        """Define default value for url field."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return {"@id": self.url_id}
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")

    @property
    def value(self):
        """Returns the url value as string."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return self.url_id
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")

    def __attrs_post_init__(self):
        """Post-initialize attributes."""
        if not self.url:
            self.url = self.default_url()
        elif isinstance(self.url, dict):
            if "_id" in self.url:
                self.url["@id"] = self.url.pop("_id")
            self.url_id = self.url["@id"]
        elif isinstance(self.url, str):
            self.url_str = self.url

        if not self._id or self._id.startswith("_:"):
            self._id = self.default_id()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return OldUrlSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return OldUrlSchema().dump(self)


def _convert_creators(value):
    """Convert creators."""
    if isinstance(value, dict):  # compatibility with previous versions
        return [Person.from_jsonld(value)]

    if isinstance(value, list):
        return [Person.from_jsonld(v) for v in value]

    return value


class Person:
    """Represent a person."""

    __slots__ = ("affiliation", "alternate_name", "email", "id", "label", "name")

    def __init__(
        self,
        *,
        affiliation: str = None,
        alternate_name: str = None,
        email: str = None,
        id: str = None,
        label: str = None,
        name: str,
    ):
        self.validate_email(email)

        if id == "mailto:None" or not id or id.startswith("_:"):
            full_identity = Person.get_full_identity(email, affiliation, name)
            id = Person.generate_id(email, full_identity, hostname=get_host(use_project_context=False))
        label = label or name

        self.affiliation: str = affiliation
        self.alternate_name: str = alternate_name
        self.email: str = email
        self.id: str = id
        self.label: str = label
        self.name: str = name

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Person):
            return False
        return self.id == other.id and self.full_identity == other.full_identity

    def __hash__(self):
        return hash((self.id, self.full_identity))

    @staticmethod
    def generate_id(email, full_identity, hostname):
        """Generate identifier for Person."""
        if email:
            return f"mailto:{email}"

        id = full_identity or str(uuid.uuid4().hex)
        id = quote(id, safe="")

        # TODO: Remove hostname part once migrating to new metadata
        return f"https://{hostname}/persons/{id}"

    @staticmethod
    def validate_email(email):
        """Check that the email is valid."""
        if not email:
            return
        if not isinstance(email, str) or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError("Email address is invalid.")

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email)

    @property
    def short_name(self):
        """Gives full name in short form."""
        names = self.name.split()
        if len(names) == 1:
            return self.name

        last_name = names[-1]
        initials = [name[0] for name in names]
        initials.pop()

        return "{0}.{1}".format(".".join(initials), last_name)

    @property
    def full_identity(self):
        """Return name, email, and affiliation."""
        return self.get_full_identity(self.email, self.affiliation, self.name)

    @staticmethod
    def get_full_identity(email, affiliation, name):
        """Return name, email, and affiliation."""
        email = f" <{email}>" if email else ""
        affiliation = f" [{affiliation}]" if affiliation else ""
        return f"{name}{email}{affiliation}"

    @classmethod
    def from_repository(cls, repository):
        """Create an instance from a repository."""
        user = repository.get_user()
        return cls(email=user.email, name=user.name)

    @classmethod
    def from_string(cls, string):
        """Create an instance from a 'Name <email>' string."""
        regex_pattern = r"([^<>\[\]]*)" r"(?:<{1}\s*(\S+@\S+\.\S+){0,1}\s*>{1}){0,1}\s*" r"(?:\[{1}(.*)\]{1}){0,1}"
        name, email, affiliation = re.search(regex_pattern, string).groups()
        if name:
            name = name.strip()
        if affiliation:
            affiliation = affiliation.strip()
        affiliation = affiliation or None

        return cls(affiliation=affiliation, email=email, name=name)

    @classmethod
    def from_dict(cls, data):
        """Create and instance from a dictionary."""
        return cls(**data)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return OldPersonSchema().load(data)


class SoftwareAgent:
    """Represent executed software."""

    __slots__ = ("id", "label")

    def __init__(self, *, id: str, label: str):
        self.id: str = id
        self.label: str = label

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, SoftwareAgent):
            return False
        return self.id == other.id and self.label == other.label

    def __hash__(self):
        return hash((self.id, self.label))

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        # FIXME: This method can return a Person object but SoftwareAgent is not its super class
        author = Person.from_commit(commit)
        if commit.author != commit.committer:
            return cls(label=commit.committer.name, id=commit.committer.email)
        return author


@attr.s
class CreatorMixin:
    """Mixin for handling creators container."""

    creators = attr.ib(kw_only=True, converter=_convert_creators)

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ", ".join(creator.name for creator in self.creators)

    @property
    def creators_full_csv(self):
        """Comma-separated list of creators with full identity."""
        return ", ".join(creator.full_identity for creator in self.creators)


def _extract_doi(value):
    """Return either a string or the doi part of a URL."""
    value = str(value)
    if is_doi(value):
        return extract_doi(value)
    return value


@attr.s(slots=True)
class DatasetTag(object):
    """Represents a Tag of an instance of a dataset."""

    name = attr.ib(default=None, kw_only=True, validator=instance_of(str))

    description = attr.ib(default=None, kw_only=True, validator=instance_of(str))

    commit = attr.ib(default=None, kw_only=True, validator=instance_of(str))

    created = attr.ib(converter=parse_date, kw_only=True)

    dataset = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    def default_id(self):
        """Define default value for id field."""
        return generate_dataset_tag_id(name=self.name, commit=self.commit)

    def __attrs_post_init__(self):
        """Post-Init hook."""
        if not self._id or self._id.startswith("_:"):
            self._id = self.default_id()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return OldDatasetTagSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return OldDatasetTagSchema().dump(self)


@attr.s(slots=True)
class Language:
    """Represent a language of an object."""

    alternate_name = attr.ib(default=None, kw_only=True)
    name = attr.ib(default=None, kw_only=True)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return OldLanguageSchema().load(data)


def convert_filename_path(p):
    """Return name of the file."""
    if p:
        return Path(p).name


def convert_based_on(v):
    """Convert based_on to DatasetFile."""
    if v:
        return DatasetFile.from_jsonld(v)


@attr.s(slots=True)
class DatasetFile(Entity):
    """Represent a file in a dataset."""

    added = attr.ib(converter=parse_date, kw_only=True)

    checksum = attr.ib(default=None, kw_only=True)

    filename = attr.ib(kw_only=True, converter=convert_filename_path)

    name = attr.ib(kw_only=True, default=None)

    filesize = attr.ib(default=None, kw_only=True)

    filetype = attr.ib(default=None, kw_only=True)

    url = attr.ib(default=None, kw_only=True)

    based_on = attr.ib(default=None, kw_only=True, converter=convert_based_on)

    external = attr.ib(default=False, kw_only=True)

    source = attr.ib(default=None, kw_only=True)

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @filename.default
    def default_filename(self):
        """Generate default filename based on path."""
        if self.path:
            return Path(self.path).name

    def default_url(self):
        """Generate default url based on project's ID."""
        return generate_dataset_file_url(filepath=self.path)

    @property
    def commit_sha(self):
        """Return commit hash."""
        return self.commit.hexsha if self.commit else ""

    @property
    def full_path(self):
        """Return full path in the current reference frame."""
        path = project_context.path / self.path
        return Path(os.path.abspath(path))

    @property
    def filesize(self):
        """Return file size."""
        return None if self.filesize is None else self.filesize

    def __attrs_post_init__(self):
        """Set the property "name" after initialization."""
        super().__attrs_post_init__()

        if not self.filename:
            self.filename = self.default_filename()

        if not self.name:
            self.name = self.filename

        parsed_id = urlparse(self._id)

        if not parsed_id.scheme:
            self._id = "file://{}".format(self._id)

        if not self.url:
            self.url = self.default_url()

    def update_commit(self, commit):
        """Set commit and update associated fields."""
        self.commit = commit
        self._id = self.default_id()
        self._label = self.default_label()

    def update_metadata(self, path, commit):
        """Update files metadata."""
        self.path = str((project_context.path / path).relative_to(project_context.path))
        self.update_commit(commit)
        self.filename = self.default_filename()
        self.url = self.default_url()
        self.added = self._now()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return OldDatasetFileSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return OldDatasetFileSchema().dump(self)


def _convert_dataset_files(value):
    """Convert dataset files."""
    coll = value

    if isinstance(coll, dict):  # compatibility with previous versions
        if any([key.startswith("@") for key in coll.keys()]):
            return [DatasetFile.from_jsonld(coll)]
        else:
            coll = value.values()

    return [DatasetFile.from_jsonld(v) for v in coll]


def _convert_dataset_tags(value):
    """Convert dataset tags."""
    if isinstance(value, dict):  # compatibility with previous versions
        value = [value]

    return [DatasetTag.from_jsonld(v) for v in value]


def _convert_language(obj):
    """Convert language object."""
    return Language.from_jsonld(obj) if isinstance(obj, dict) else obj


def _convert_keyword(keywords):
    """Convert keywords collection."""
    if isinstance(keywords, (list, tuple)):
        return keywords

    if isinstance(keywords, dict):
        return keywords.keys()


@attr.s
class Dataset(Entity, CreatorMixin):
    """Represent a dataset."""

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    date_published = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    identifier = attr.ib(default=attr.Factory(uuid.uuid4), kw_only=True, converter=_extract_doi)

    in_language = attr.ib(default=None, converter=_convert_language, kw_only=True)

    images = attr.ib(default=None, kw_only=True)

    keywords = attr.ib(converter=_convert_keyword, kw_only=True, default=None)

    license = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, type=str, kw_only=True)

    url = attr.ib(default=None, kw_only=True)

    version = attr.ib(default=None, kw_only=True)

    date_created = attr.ib(converter=parse_date, kw_only=True)

    files = attr.ib(factory=list, converter=_convert_dataset_files, kw_only=True)

    tags = attr.ib(factory=list, converter=_convert_dataset_tags, kw_only=True)

    same_as = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, kw_only=True)

    derived_from = attr.ib(default=None, kw_only=True)

    immutable = attr.ib(default=False, kw_only=True)

    _modified = attr.ib(default=False, init=False)

    _mutated = attr.ib(default=False, init=False)

    _metadata_path = attr.ib(default=None, init=False)

    @date_created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self.identifier):
            return self.identifier
        return str(self.identifier)[:8]

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ", ".join(creator.name for creator in self.creators)

    @property
    def keywords_csv(self):
        """Comma-separated list of keywords associated with dataset."""
        return ", ".join(self.keywords)

    @property
    def tags_csv(self):
        """Comma-separated list of tags associated with dataset."""
        return ",".join(tag.name for tag in self.tags)

    @property
    def initial_identifier(self):
        """Return the first identifier of the dataset."""
        if self.path:
            return Path(self.path).name

    def contains_any(self, files):
        """Check if files are already within a dataset."""
        for file_ in files:
            if self.find_file(file_["path"]):
                return True
        return False

    def find_files(self, paths):
        """Return all paths that are in files container."""
        files_paths = {str(project_context.path / f.path) for f in self.files}
        return {p for p in paths if str(p) in files_paths}

    def find_file(self, path, return_index=False):
        """Find a file in files container using its relative path."""
        for index, file in enumerate(self.files):
            if str(file.path) == str(path):
                if return_index:
                    return index
                return file

    def update_metadata(self, **kwargs):
        """Updates instance attributes."""
        for attribute, value in kwargs.items():
            if value and value != getattr(self, attribute):
                self._modified = True
                setattr(self, attribute, value)

        return self

    def update_files(self, files):
        """Update files with collection of DatasetFile objects."""
        if isinstance(files, DatasetFile):
            files = (files,)

        new_files = []

        for new_file in files:
            old_file = self.find_file(new_file.path)
            if not old_file:
                new_files.append(new_file)
            elif new_file.commit != old_file.commit or new_file.added != old_file.added:
                self.unlink_file(new_file.path)
                new_files.append(new_file)

        if not new_files:
            return

        self._modified = True
        self.files += new_files

    def unlink_file(self, path, missing_ok=False):  # FIXME: Remove unused code
        """Unlink a file from dataset.

        Args:
            path: Relative path used as key inside files container.
            missing_ok (bool): Whether to ignore missing files or raise an error (Default value = False).
        """
        index = self.find_file(path, return_index=True)
        if index is not None:
            self._modified = True
            return self.files.pop(index)

        if not missing_ok:
            raise errors.InvalidFileOperation(f"File cannot be found: {path}")

    def mutate(self):
        """Update mutation history and assign a new identifier.

        Do not mutate more than once before committing the metadata or otherwise there would be missing links in the
        chain of changes.
        """
        if self.immutable:
            raise errors.OperationError(f"Cannot mutate an immutable dataset: {self.name}")

        # As a safetynet, we only allow one mutation during lifetime of a dataset object; this is not 100% error-proof
        # because one can create a new object from a mutated but uncommitted metadata file.
        if self._mutated:
            return
        self._mutated = True

        self.same_as = None
        self.derived_from = Url(url_id=self._id)

        repository = project_context.repository
        mutator = Person.from_repository(repository)
        if not any(c for c in self.creators if c.email == mutator.email):
            self.creators.append(mutator)

        self.date_created = self._now()
        self.date_published = None

        self._replace_identifier(new_identifier=str(uuid.uuid4()))

    def _replace_identifier(self, new_identifier):
        """Replace identifier and update all related fields."""
        self.identifier = new_identifier
        self._set_id()
        self.url = self._id
        self._label = self.identifier

    def _set_id(self):
        self._id = generate_dataset_id(identifier=self.identifier)

    def __attrs_post_init__(self):
        """Post-Init hook."""
        super().__attrs_post_init__()

        self._set_id()
        self.url = self._id
        self._label = self.identifier

        if self.derived_from:
            host = get_host()
            derived_from_id = self.derived_from._id
            derived_from_url = self.derived_from.url.get("@id")
            u = urlparse(derived_from_url)
            derived_from_url = u._replace(netloc=host).geturl()
            self.derived_from = Url(id=derived_from_id, url_id=derived_from_url)

        # if `date_published` is set, we are probably dealing with
        # an imported dataset so `date_created` is not needed
        if self.date_published:
            self.date_created = None

        if not self.path:
            absolute_path = LinkReference(
                metadata_path=project_context.metadata_path, name=f"datasets/{self.name}"
            ).reference.parent
            self.path = str(absolute_path.relative_to(project_context.path))

        if project_context.has_context():
            try:
                revision = self.commit.hexsha if self.commit else "HEAD"
                repository = project_context.repository
                self.commit = repository.get_previous_commit(os.path.join(self.path, "metadata.yml"), revision=revision)
            except errors.GitCommitNotFoundError:
                pass

        if not self.name:
            self.name = generate_default_name(self.title, self.version)

    @classmethod
    def from_yaml(cls, path, commit=None):
        """Return an instance from a YAML file."""
        data = yaml.read_yaml(path)

        self = cls.from_jsonld(data=data, commit=commit)
        self._metadata_path = path

        return self

    @classmethod
    def from_jsonld(cls, data, commit=None, schema_class=None):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, (dict, list)):
            raise ValueError(data)

        schema_class = schema_class or OldDatasetSchema
        return schema_class(commit=commit, flattened=True).load(data)

    def to_yaml(self, path=None, immutable=False):
        """Write an instance to the referenced YAML file."""
        if self._modified and not (immutable or self.immutable):
            self.mutate()

        self._metadata_path = path or self._metadata_path
        data = OldDatasetSchema(flattened=True).dump(self)
        yaml.write_yaml(path=self._metadata_path, data=data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return OldDatasetSchema(flattened=True).dump(self)


class ImageObject:
    """Represents a schema.org `ImageObject`."""

    def __init__(self, content_url: str, position: int, id=None):
        self.content_url = content_url
        self.position = position
        self.id = id

    @staticmethod
    def generate_id(dataset: Dataset, position: int) -> str:
        """Generate @id field."""
        return urljoin(dataset._id + "/", posixpath.join("images", str(position)))

    @property
    def is_absolute(self):
        """Whether content_url is an absolute or relative url."""
        return bool(urlparse(self.content_url).netloc)


class OldPersonSchema(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    affiliation = StringList(schema.affiliation, load_default=None)
    alternate_name = StringList(schema.alternateName, load_default=None)
    email = fields.String(schema.email, load_default=None)
    id = fields.Id()
    label = StringList(rdfs.label, load_default=None)
    name = StringList(schema.name, load_default=None)


class ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = Project
        unknown = EXCLUDE

    name = fields.String(schema.name, load_default=None)
    created = DateTimeList(schema.dateCreated, load_default=None, format="iso", extra_formats=("%Y-%m-%d",))
    version = StringList(schema.schemaVersion, load_default="1")
    agent_version = StringList(schema.agent, load_default="pre-0.11.0")
    template_source = fields.String(renku.templateSource, load_default=None)
    template_ref = fields.String(renku.templateReference, load_default=None)
    template_id = fields.String(renku.templateId, load_default=None)
    template_version = fields.String(renku.templateVersion, load_default=None)
    template_metadata = fields.String(renku.templateMetadata, load_default=None)
    immutable_template_files = fields.List(renku.immutableTemplateFiles, fields.String(), load_default=[])
    automated_update = fields.Boolean(renku.automatedTemplateUpdate, load_default=True)
    creator = Nested(schema.creator, OldPersonSchema, load_default=None)
    _id = fields.Id(init_name="id", load_default=None)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.created = fix_datetime(obj.created)
        return obj


class OldCommitMixinSchema(JsonLDSchema):
    """CommitMixin schema."""

    class Meta:
        """Meta class."""

        model = CommitMixin

    path = fields.String(prov.atLocation)
    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label", load_default=None)
    _project = Nested(
        schema.isPartOf, [ProjectSchema, "V9ProjectSchema", V10ProjectSchema], init_name="project", load_default=None
    )


class OldEntitySchema(OldCommitMixinSchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, wfprov.Artifact]
        model = Entity

    checksum = fields.String(renku.checksum, load_default=None)


class OldCollectionSchema(OldEntitySchema):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Collection]
        model = Collection

    members = Nested(prov.hadMember, [OldEntitySchema, "OldCollectionSchema"], many=True)


class OldSoftwareAgentSchema(JsonLDSchema):
    """SoftwareAgent schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.SoftwareAgent, wfprov.WorkflowEngine]
        model = SoftwareAgent
        unknown = EXCLUDE

    label = fields.String(rdfs.label)
    id = fields.Id()


class OldCreatorMixinSchema(JsonLDSchema):
    """CreatorMixin schema."""

    class Meta:
        """Meta class."""

        unknown = EXCLUDE

    creators = Nested(schema.creator, OldPersonSchema, many=True)


class OldUrlSchema(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    url = Uri(schema.url, load_default=None)
    _id = fields.Id(init_name="id", load_default=None)


class OldDatasetTagSchema(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    name = fields.String(schema.name)
    description = fields.String(schema.description, load_default=None)
    commit = fields.String(schema.location)
    created = fields.DateTime(schema.startDate, load_default=None, format="iso", extra_formats=("%Y-%m-%d",))
    dataset = fields.String(schema.about)
    _id = fields.Id(init_name="id")

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        object.__setattr__(obj, "created", fix_datetime(obj.created))
        return obj


class OldLanguageSchema(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class OldDatasetFileSchema(OldEntitySchema):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.DigitalDocument
        model = DatasetFile
        unknown = EXCLUDE

    added = DateTimeList(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
    name = fields.String(schema.name, load_default=None)
    url = fields.String(schema.url, load_default=None)
    based_on = Nested(schema.isBasedOn, "OldDatasetFileSchema", load_default=None)
    external = fields.Boolean(renku.external, load_default=False)
    source = fields.String(renku.source, load_default=None)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.added = fix_datetime(obj.added)
        return obj


class OldImageObjectSchema(JsonLDSchema):
    """ImageObject schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.ImageObject
        model = ImageObject
        unknown = EXCLUDE

    id = fields.Id(load_default=None)
    content_url = fields.String(schema.contentUrl)
    position = fields.Integer(schema.position)


class OldDatasetSchema(OldEntitySchema, OldCreatorMixinSchema):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    _id = fields.Id(init_name="id", load_default=None)
    _label = fields.String(rdfs.label, init_name="label", load_default=None)
    date_published = fields.DateTime(
        schema.datePublished,
        load_default=None,
        allow_none=True,
        format="%Y-%m-%d",
        extra_formats=("iso", "%Y-%m-%dT%H:%M:%S"),
    )
    description = fields.String(schema.description, load_default=None)
    identifier = fields.String(schema.identifier)
    in_language = Nested(schema.inLanguage, OldLanguageSchema, load_default=None)
    images = fields.Nested(schema.image, OldImageObjectSchema, many=True, load_default=None, allow_none=True)
    keywords = fields.List(schema.keywords, fields.String(), allow_none=True, load_default=None)
    license = Uri(schema.license, allow_none=True, load_default=None)
    title = fields.String(schema.name)
    url = fields.String(schema.url, load_default=None)
    version = fields.String(schema.version, load_default=None)
    date_created = fields.DateTime(
        schema.dateCreated, load_default=None, allow_none=True, format="iso", extra_formats=("%Y-%m-%d",)
    )
    files = Nested(schema.hasPart, OldDatasetFileSchema, many=True)
    tags = Nested(schema.subjectOf, OldDatasetTagSchema, many=True)
    same_as = Nested(schema.sameAs, OldUrlSchema, load_default=None)
    name = fields.String(schema.alternateName)
    derived_from = Nested(prov.wasDerivedFrom, OldUrlSchema, load_default=None)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.date_published = fix_datetime(obj.date_published)
        obj.date_created = fix_datetime(obj.date_created)
        return obj


def get_project_datasets():
    """Return Dataset migration models for a project."""
    paths = get_datasets_path().rglob(OLD_METADATA_PATH)
    return [Dataset.from_yaml(path=path) for path in paths]


def generate_label(path, hexsha):
    """Generate label field."""
    return f"{path}@{hexsha}"


def generate_file_id(hexsha, path):
    """Generate DatasetFile id field."""
    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"
    host = project_context.remote.host or host
    host = os.environ.get("RENKU_DOMAIN") or host

    # TODO: Use plural name for entity id: /blob/ -> /blobs/
    # always set the id by the identifier
    return urljoin(f"https://{host}", posixpath.join(f"/blob/{hexsha}/{quote(str(path))}"))


class MappedIOStreamSchema(JsonLDSchema):
    """MappedIOStream schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.IOStream]
        model = MappedIOStream
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    stream_type = fields.String(renku.streamType)


class CommandParameterSchema(JsonLDSchema):
    """CommandParameter schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandParameter]  # , schema.PropertyValueSpecification]
        model = CommandParameter
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    default_value = fields.Raw(schema.defaultValue, load_default=None)
    description = fields.String(schema.description, load_default=None)
    name = fields.String(schema.name, load_default=None)
    position = fields.Integer(renku.position, load_default=None)
    prefix = fields.String(renku.prefix, load_default=None)


class CommandArgumentSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandArgument]
        model = CommandArgument
        unknown = EXCLUDE

    value = fields.String(renku.value)


class CommandInputSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandInput]
        model = CommandInput
        unknown = EXCLUDE

    consumes = Nested(renku.consumes, [OldEntitySchema, OldCollectionSchema])
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, load_default=None)


class CommandOutputSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandOutput]
        model = CommandOutput
        unknown = EXCLUDE

    create_folder = fields.Boolean(renku.createFolder)
    produces = Nested(renku.produces, [OldEntitySchema, OldCollectionSchema])
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, load_default=None)


class RunParameterSchema(JsonLDSchema):
    """RunParameter schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.RunParameter]
        model = RunParameter
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    name = fields.String(schema.name)
    value = fields.String(renku.value)
    type = fields.String(renku.type)


class RunSchema(OldCommitMixinSchema):
    """Run schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.Run, prov.Plan, prov.Entity]
        model = Run
        unknown = EXCLUDE

    command = fields.String(renku.command, load_default=None)
    successcodes = fields.List(renku.successCodes, fields.Integer(), load_default=[0])
    subprocesses = Nested(renku.hasSubprocess, nested="OrderedSubprocessSchema", load_default=None, many=True)
    arguments = Nested(renku.hasArguments, CommandArgumentSchema, many=True, load_default=None)
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, load_default=None)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, load_default=None)
    run_parameters = Nested(renku.hasRunParameters, RunParameterSchema, many=True, load_default=None)
    name = fields.String(schema.name, load_default=None)
    description = fields.String(schema.description, load_default=None)
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)


class OrderedSubprocessSchema(JsonLDSchema):
    """OrderedSubprocess schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.OrderedSubprocess]
        model = OrderedSubprocess
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    index = fields.Integer(renku.index)
    process = Nested(renku.process, RunSchema)


class AssociationSchema(JsonLDSchema):
    """Association schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Association
        model = Association
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    plan = Nested(prov.hadPlan, [RunSchema])
    agent = Nested(prov.agent, [OldSoftwareAgentSchema, OldPersonSchema])


class UsageSchema(JsonLDSchema):
    """Usage schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Usage
        model = Usage
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    entity = Nested(prov.entity, [OldEntitySchema, OldCollectionSchema, OldDatasetSchema, OldDatasetFileSchema])
    role = fields.String(prov.hadRole, load_default=None)


class GenerationSchema(JsonLDSchema):
    """Generation schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Generation
        model = Generation
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    entity = Nested(
        prov.qualifiedGeneration,
        [OldEntitySchema, OldCollectionSchema, OldDatasetSchema, OldDatasetFileSchema],
        reverse=True,
    )
    role = fields.String(prov.hadRole, load_default=None)


class ActivitySchema(OldCommitMixinSchema):
    """Activity schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Activity
        model = Activity
        unknown = EXCLUDE

    _message = fields.String(rdfs.comment, init_name="message", load_default=None)
    _was_informed_by = fields.List(prov.wasInformedBy, fields.IRI(), init_name="was_informed_by")
    generated = Nested(prov.activity, GenerationSchema, reverse=True, many=True, load_default=None)
    invalidated = Nested(
        prov.wasInvalidatedBy, [OldEntitySchema, OldCollectionSchema], reverse=True, many=True, load_default=None
    )
    influenced = Nested(prov.influenced, OldCollectionSchema, many=True)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
    ended_at_time = fields.DateTime(prov.endedAtTime, add_value_types=True)
    agents = Nested(prov.wasAssociatedWith, [OldPersonSchema, OldSoftwareAgentSchema], many=True)


class ProcessRunSchema(ActivitySchema):
    """ProcessRun schema."""

    class Meta:
        """Meta class."""

        rdf_type = wfprov.ProcessRun
        model = ProcessRun
        unknown = EXCLUDE

    association = Nested(prov.qualifiedAssociation, AssociationSchema)
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    qualified_usage = Nested(prov.qualifiedUsage, UsageSchema, many=True)
    run_parameter = Nested(renku.hasRunParameter, RunParameterSchema, many=True)


class WorkflowRunSchema(ProcessRunSchema):
    """WorkflowRun schema."""

    class Meta:
        """Meta class."""

        rdf_type = wfprov.WorkflowRun
        model = WorkflowRun
        unknown = EXCLUDE

    _processes = Nested(wfprov.wasPartOfWorkflowRun, ProcessRunSchema, reverse=True, many=True, init_name="processes")


class V9ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = new_schema.Project
        unknown = EXCLUDE

    agent_version = StringList(schema.agent, load_default="pre-0.11.0")
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    automated_update = fields.Boolean(renku.automatedTemplateUpdate, load_default=True)
    creator = Nested(schema.creator, PersonSchema, load_default=None)
    date_created = DateTimeList(schema.dateCreated, load_default=None, format="iso", extra_formats=("%Y-%m-%d",))
    description = fields.String(schema.description, load_default=None)
    id = fields.Id(load_default=None)
    immutable_template_files = fields.List(renku.immutableTemplateFiles, fields.String(), load_default=list())
    name = fields.String(schema.name, load_default=None)
    template_id = fields.String(renku.templateId, load_default=None)
    template_metadata = fields.String(renku.templateMetadata, load_default=None)
    template_ref = fields.String(renku.templateReference, load_default=None)
    template_source = fields.String(renku.templateSource, load_default=None)
    template_version = fields.String(renku.templateVersion, load_default=None)
    version = StringList(schema.schemaVersion, load_default="1")
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)
