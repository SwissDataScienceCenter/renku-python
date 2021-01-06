# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Support JSON-LD context in models."""

from pathlib import Path

import yaml

try:
    from yaml import CDumper as Dumper
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import Dumper, SafeLoader


# Shamelessly copy/pasting from SO:
# https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
# This is needed to allow us to load from yaml and use json down the line.
class NoDatesSafeLoader(SafeLoader):
    """Used to safely load basic python objects but ignore datetime strings."""

    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Remove implicit resolvers for a particular tag.

        Takes care not to modify resolvers in super classes.

        We want to load datetimes as strings, not dates, because we
        go on to serialise as json which doesn't have the advanced types
        of yaml, and leads to incompatibilities down the track.
        """
        if "yaml_implicit_resolvers" not in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = [
                (tag, regexp) for tag, regexp in mappings if tag != tag_to_remove
            ]


NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")


def read_yaml(path):
    """Load YAML file and return its content as a dict."""
    with Path(path).open(mode="r") as fp:
        return load_yaml(fp)


def write_yaml(path, data):
    """Store data to a YAML file."""

    Dumper.ignore_aliases = lambda _, data: True

    with Path(path).open("w") as fp:
        yaml.dump(data, fp, default_flow_style=False, Dumper=Dumper)


def load_yaml(data):
    """Load YAML data and return its content as a dict."""
    return yaml.load(data, Loader=NoDatesSafeLoader) or {}
