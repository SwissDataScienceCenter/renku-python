# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Command builder for local object database."""

import threading

import inject

from renku.core.incubation.database import Database, Storage
from renku.core.management.command_builder.command import Command, check_finalized

_LOCAL = threading.local()


def database():
    return getattr(_LOCAL, "database", None)


class DatabaseCommand(Command):
    """Builder to get a database connection."""

    PRE_ORDER = 3
    POST_ORDER = 5

    def __init__(self, builder, write=False, path=None):
        """__init__ of ProjectLock."""
        self._builder = builder
        self._write = write
        self._path = path

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Lock the project."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")

        client = context["client"]
        storage = Storage(self._path or client.database_path)

        _LOCAL.database = Database(storage=storage)
        inject.configure(lambda binder: binder.bind_to_provider(Database, database))

    def _post_hook(self, builder, context, result, *args, **kwargs):
        if self._write:
            _LOCAL.database.commit()

        del _LOCAL.database

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.PRE_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.POST_ORDER, self._post_hook)

        return self._builder.build()
