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
"""Click utilities."""

import click


class CaseInsensitiveChoice(click.Choice):
    """Case-insensitive click choice.

    Based on https://github.com/pallets/click/issues/569.
    """

    def convert(self, value, param, ctx):
        """Convert value to its choice value."""
        if value is None:
            return None
        return super(CaseInsensitiveChoice, self).convert(value.lower(), param, ctx)


class MutuallyExclusiveOption(click.Option):
    """Custom option class to allow specifying mutually exclusive options in click commands."""

    def __init__(self, *args, **kwargs):
        mutually_exclusive = sorted(kwargs.pop("mutually_exclusive", []))
        self.mutually_exclusive = set()
        self.mutually_exclusive_names = []

        for mutex in mutually_exclusive:
            if type(mutex) == tuple:
                self.mutually_exclusive.add(mutex[0])
                self.mutually_exclusive_names.append(mutex[1])
            else:

                self.mutually_exclusive.add(mutex)
                self.mutually_exclusive_names.append(mutex)

        _help = kwargs.get("help", "")
        if self.mutually_exclusive:
            ex_str = ", ".join(self.mutually_exclusive_names)
            kwargs["help"] = f"{_help} NOTE: This argument is mutually exclusive with arguments: [{ex_str}]."
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        """Handles the parse result for the option."""
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise click.UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(self.name, ", ".join(sorted(self.mutually_exclusive_names)))
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)
