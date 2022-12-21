# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
"""Renku modules."""

from __future__ import absolute_import, print_function

import importlib
import importlib.util
import sys
import warnings

from renku.version import __template_version__, __version__


class LoaderWrapper(importlib.abc.Loader):
    """Wrap an importlib loader and add the loaded module to sys.modules with an additional name."""

    def __init__(self, wrapped_loader, additional_namespace, original_namespace):
        self.wrapped_loader = wrapped_loader
        self.original_namespace = original_namespace
        self.additional_namespace = additional_namespace

    def get_code(self, fullname):
        """Get executable code.

        Needs to be patched with target name to bypass check in python for names having to match.
        """
        target_name = self.original_namespace + fullname[len(self.additional_namespace) :]
        return self.wrapped_loader.get_code(target_name)

    def create_module(self, spec):
        """Create a module from spec."""
        if spec.name in sys.modules:
            # NOTE: Original name was already loaded, no need for going through all of importlib
            return sys.modules[spec.name]
        return self.wrapped_loader.create_module(spec)

    def exec_module(self, module):
        """Load the module using the wrapped loader."""
        self.wrapped_loader.exec_module(module)
        sys.modules[self.additional_namespace] = module

    def __getattr__(self, name):
        """Forward all calls to wrapped loader except for one implemented here."""
        if name in ["exec_module", "create_module", "get_code"]:
            object.__getattribute__(self, name)

        return getattr(self.wrapped_loader, name)


class DeprecatedImportInterceptor(importlib.abc.MetaPathFinder):
    """Replaces imports of deprecated modules on the fly.

    Replaces old namespace with new namespace, loads the new one, updates sys.modules with the old namespace
    pointing to the new namespace and returns the loaded module.
    """

    def __init__(self, package_redirects):
        self.package_redirects = package_redirects

    def find_spec(self, fullname, path, target=None):
        """Find the spec for a namespace."""
        match = next((n for n in self.package_redirects if fullname.startswith(n)), None)
        if match is not None:
            if match == fullname and self.package_redirects[match][1]:
                warnings.warn(
                    f"The {fullname} module has moved to {self.package_redirects[match][0]} and is deprecated",
                    DeprecationWarning,
                    stacklevel=2,
                )
            try:
                subpath = fullname[len(match) :]
                target_name = self.package_redirects[match][0] + subpath

                sys.meta_path = [x for x in sys.meta_path if x is not self]
                spec = importlib.util.find_spec(target_name)
                if spec is None:
                    return None

                spec.loader = LoaderWrapper(spec.loader, fullname, target_name)
            finally:
                sys.meta_path.insert(0, self)
            return spec

        return None


# NOTE: Patch python import machinery with custom loader
sys.meta_path.insert(
    0,
    DeprecatedImportInterceptor(
        {
            "renku.core.models": ("renku.domain_model", False),
            "renku.core.metadata": ("renku.infrastructure", False),
            "renku.core.commands": ("renku.command", True),
            "renku.core.plugins": ("renku.core.plugin", True),
            "renku.api": ("renku.ui.api", False),
            "renku.cli": ("renku.ui.cli", True),
        }
    ),
)

__all__ = ("__template_version__", "__version__")
