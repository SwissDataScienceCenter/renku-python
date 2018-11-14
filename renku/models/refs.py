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
"""Manage names of Renku objects."""

import os
import subprocess

import attr

from renku import errors
from renku._compat import Path


@attr.s(slots=True)
class LinkReference:
    """Manage linked object names."""

    client = attr.ib()
    name = attr.ib()

    REFS = 'refs'
    """Define a name of the folder with references in the Renku folder."""

    @classmethod
    def check_ref_format(cls, name):
        r"""Ensures that a reference name is well formed.

        It follows Git naming convention:

        - any path component of it begins with ".", or
        - it has double dots "..", or
        - it has ASCII control characters, or
        - it has ":", "?", "[", "\", "^", "~", SP, or TAB anywhere, or
        - it has "*" anywhere, or
        - it ends with a "/", or
        - it ends with ".lock", or
        - it contains a "@{" portion
        """
        return subprocess.run(('git', 'check-ref-format',
                               name)).returncode == 0

    @name.validator
    def name_validator(self, attribute, value):
        """Validate reference name."""
        if not self.check_ref_format(value):
            raise errors.UsageError(
                'The reference name "{0}" is not valid.'.format(value)
            )

    @property
    def path(self):
        """Return full reference path."""
        return self.client.renku_path / self.REFS / self.name

    @property
    def reference(self):
        """Return the path we point to relative to the client."""
        return self.path.resolve()

    def delete(self):
        """Delete the reference at the given path."""
        return self.path.unlink()

    def set_reference(self, reference):
        """Set ourselves to the given reference path."""
        reference_path = Path(reference).resolve().absolute()
        reference_path.relative_to(self.client.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(
            os.path.relpath(str(reference_path), start=str(self.path.parent)),
            str(self.path)
        )

    @classmethod
    def iter_items(cls, client, common_path=None):
        """Find all references in the repository."""
        refs_path = path = client.renku_path / cls.REFS
        if common_path:
            path = path / common_path

        for name in path.rglob('*'):
            yield cls(client=client, name=str(name.relative_to(refs_path)))

    @classmethod
    def create(cls, client, name, force=False):
        """Create symlink to object in reference path."""
        ref = cls(client=client, name=name)
        path = ref.path

        if not force and path.exists():
            raise OSError(str(path))
        elif force and path.exists():
            ref.delete()

        return ref

    def rename(self, new_name, force=False):
        """Rename self to a new name."""
        new_ref = self.create(client=self.client, name=new_name, force=force)
        new_ref.set_reference(self.reference)
        self.delete()
        return new_ref
