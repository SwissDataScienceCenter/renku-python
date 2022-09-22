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
"""Sphinx configuration."""

from __future__ import print_function

import os
import sys
from os.path import abspath, dirname, join

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

sys.path.insert(0, abspath(join(dirname(__file__))))
sys.path.append(abspath("../_ext"))

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
# needs_sphinx = '1.0'

# Do not warn on external images.
suppress_warnings = ["image.nonlocal_uri"]

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "plantweb.directive",
    "sphinx.ext.autodoc",
    "sphinx_click",
    "sphinx_tabs.tabs",
    "sphinx.ext.napoleon",
    "cheatsheet",
]
cheatsheet_target = os.environ.get("CHEATSHEET_TARGET", None)
cheatsheet_groups = [
    "Typical Workflow",
    "Getting Started",
    "Project Templates",
    "Working with Renku Datasets",
    "Running",
    "Workflows",
    "Managing Interactive Sessions",
    "Config",
    "Misc",
    "Undo Renku Command",
]


html_theme_options = {
    # Disable showing the sidebar. Defaults to 'false'
    "nosidebar": True,
}

# Add any paths that contain templates here, relative to this directory.
# templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
# source_suffix = ['.rst', '.md']
source_suffix = ".rst"

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = "cheatsheet"

# General information about the project.
project = "Renku"
copyright = "2017-2022, Swiss Data Science Center"
author = "Swiss Data Science Center"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.

# Get the version string.
version = version("renku")

# The full version, including alpha/beta/rc tags.
release = version

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build", "CHANGES.rst"]

# The reST default role (used for this markup: `text`) to use for all
# documents.
# default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
# add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
# show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
# pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
# keep_warnings = False


# -- Options for LaTeX output ---------------------------------------------

templates_path = ["../_templates"]

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_engine = "xelatex"

latex_documents = [
    (master_doc, "cheatsheet.tex", "Cheatsheet", "SDSC", "sdsc"),
]

latex_additional_files = [
    "../_static/cheatsheet/sdsc_cheatsheet.cls",
    "../_static/cheatsheet/images/Background.pdf",
    "../_static/cheatsheet/images/EPFL.png",
    "../_static/cheatsheet/images/ETHZ.png",
    "../_static/cheatsheet/images/SDSC.png",
    "../_static/cheatsheet/images/RenkuLogo.pdf",
    "../_static/cheatsheet/fonts/Inconsolata.ttf",
    "../_static/cheatsheet/fonts/SourceSans3.ttf",
    "../_static/cheatsheet/fonts/SourceSans3-Bold.ttf",
    "../_static/cheatsheet/fonts/SourceSans3-Italic.ttf",
    "../_static/cheatsheet/fonts/SourceSans3-BoldItalic.ttf",
]


# Autodoc configuration.
autoclass_content = "both"
autodoc_mock_imports = ["persistent", "ZODB"]
autodoc_typehints = "none"
autodoc_typehints_description_target = "documented"

# Napoleon (Google style) settings
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# sphinx type references only work for types that documentation is generated for
# Suppress warnings for these types that are referenced but not documented themselves.
nitpick_ignore = [
    ("py:class", "CommandResult"),
    ("py:class", "CommunicationCallback"),
    ("py:class", "datetime"),
    ("py:class", "DiGraph"),
    ("py:class", "DynamicProxy"),
    ("py:class", "IActivityGateway"),
    ("py:class", "IClientDispatcher"),
    ("py:class", "IDatabaseDispatcher"),
    ("py:class", "IDatasetGateway"),
    ("py:class", "IPlanGateway"),
    ("py:class", "LocalClient"),
    ("py:class", "NoValueType"),
    ("py:class", "OID_TYPE"),
    ("py:class", "Path"),
    ("py:class", "Persistent"),
    ("py:class", "optional"),
    ("py:class", '"LocalClient"'),
    ("py:class", '"ValueResolver"'),
    ("py:exc", "errors.ParameterError"),
]

nitpick_ignore_regex = [
    ("py:class", r"calamus.*"),
    ("py:class", r"docker.*"),
    ("py:class", r"marshmallow.*"),
    ("py:class", r"persistent.*"),
    ("py:class", r"yaml.*"),
]
