# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Represents a workflow template."""

import uuid

from renku.core.models import jsonld as jsonld


@jsonld.s(
    type=["renku:IOStream",],
    context={
        "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        "prov": "http://www.w3.org/ns/prov#",
    },
    cmp=False,
)
class MappedIOStream(object):
    """Represents an IO stream (stdin, stdout, stderr)."""

    _id = jsonld.ib(context="@id", kw_only=True)
    _label = jsonld.ib(default=None, context="rdfs:label", kw_only=True)

    STREAMS = ["stdin", "stdout", "stderr"]

    stream_type = jsonld.ib(
        context={"@id": "renku:streamType", "@type": "http://www.w3.org/2001/XMLSchema#string",},
        type=str,
        kw_only=True,
    )

    @_id.default
    def default_id(self):
        """Set default id."""
        # TODO: make bnode ids nicer once this issue is in a release:
        # https://github.com/RDFLib/rdflib/issues/888
        # right now it's limited to a-zA-Z0-9 (-_ will work once it's fixed)
        return "_:MappedIOStream-{}".format(str(uuid.uuid4())).replace("-", "")

    def default_label(self):
        """Set default label."""
        return 'Stream mapping for stream "{}"'.format(self.stream_type)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._label:
            self._label = self.default_label()


@jsonld.s(
    type=["renku:CommandParameter",],
    context={
        "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        "prov": "http://www.w3.org/ns/prov#",
    },
    cmp=False,
)
class CommandParameter(object):
    """Represents a parameter for an execution template."""

    _id = jsonld.ib(default=None, context="@id", kw_only=True)
    _label = jsonld.ib(default=None, context="rdfs:label", kw_only=True)

    position = jsonld.ib(
        default=None,
        context={"@id": "renku:position", "@type": "http://www.w3.org/2001/XMLSchema#integer",},
        type=int,
        kw_only=True,
    )

    prefix = jsonld.ib(
        default=None,
        context={"@id": "renku:prefix", "@type": "http://www.w3.org/2001/XMLSchema#string",},
        type=str,
        kw_only=True,
    )

    @property
    def sanitized_id(self):
        """Return ``_id`` sanitized for use in non-jsonld contexts."""
        return self._id.split(":", 1)[1].replace("-", "_")


@jsonld.s(
    type=["renku:CommandArgument",],
    context={
        "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        "prov": "http://www.w3.org/ns/prov#",
    },
    cmp=False,
)
class CommandArgument(CommandParameter):
    """An argument to a command that is neither input nor output."""

    value = jsonld.ib(
        default=None,
        context={"@id": "renku:value", "@type": "http://www.w3.org/2001/XMLSchema#string",},
        type=str,
        kw_only=True,
    )

    def default_id(self):
        """Set default id."""
        return "_:CommandArgument-{}".format(str(uuid.uuid4())).replace("-", "")

    def default_label(self):
        """Set default label."""
        return 'Command Argument "{}"'.format(self.value)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.value]
            return ["{}{}".format(self.prefix, self.value)]

        return [self.value]

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._id:
            self._id = self.default_id()

        if not self._label:
            self._label = self.default_label()


@jsonld.s(
    type=["renku:CommandInput",],
    context={
        "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        "prov": "http://www.w3.org/ns/prov#",
    },
    cmp=False,
)
class CommandInput(CommandParameter):
    """An input to a command."""

    consumes = jsonld.ib(
        context="renku:consumes",
        kw_only=True,
        type=["renku.core.models.entities.Entity", "renku.core.models.entities.Collection"],
    )

    mapped_to = jsonld.ib(default=None, context="prov:mappedTo", kw_only=True, type=MappedIOStream)

    def default_id(self):
        """Set default id."""
        return "_:CommandInput-{}".format(str(uuid.uuid4())).replace("-", "")

    def default_label(self):
        """Set default label."""
        return 'Command Input "{}"'.format(self.consumes.path)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.consumes.path]
            return ["{}{}".format(self.prefix, self.consumes.path)]

        return [self.consumes.path]

    def to_stream_repr(self):
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        return " < {}".format(self.consumes.path)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._id:
            self._id = self.default_id()

        if not self._label:
            self._label = self.default_label()


@jsonld.s(
    type=["renku:CommandOutput",],
    context={
        "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
        "prov": "http://www.w3.org/ns/prov#",
    },
    cmp=False,
)
class CommandOutput(CommandParameter):
    """An output of a command."""

    create_folder = jsonld.ib(default=False, context="renku:createFolder", kw_only=True, type=bool)

    produces = jsonld.ib(
        context="renku:produces",
        kw_only=True,
        type=["renku.core.models.entities.Entity", "renku.core.models.entities.Collection"],
    )

    mapped_to = jsonld.ib(default=None, context="prov:mappedTo", kw_only=True, type=MappedIOStream)

    def default_id(self):
        """Set default id."""
        return "_:CommandOutput-{}".format(str(uuid.uuid4())).replace("-", "")

    def default_label(self):
        """Set default label."""
        return 'Command Output "{}"'.format(self.produces.path)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.produces.path]
            return ["{}{}".format(self.prefix, self.produces.path)]

        return [self.produces.path]

    def to_stream_repr(self):
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        if self.mapped_to.stream_type == "stdout":
            return " > {}".format(self.produces.path)

        return " 2> {}".format(self.produces.path)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._id:
            self._id = self.default_id()

        if not self._label:
            self._label = self.default_label()
