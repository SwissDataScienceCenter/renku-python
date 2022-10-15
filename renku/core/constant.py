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
"""Renku core constants."""

import os
from pathlib import Path

APP_NAME = "Renku"
"""Application name for storing configuration."""

RENKU_HOME = ".renku"
"""Project directory name."""

RENKU_TMP = "tmp"

CACHE = "cache"
"""Directory to cache transient data."""

CONFIG_NAME = "renku.ini"
CONFIG_LOCAL_PATH = os.path.join(RENKU_HOME, CONFIG_NAME)

DATA_DIR_CONFIG_KEY = "data_directory"

DATABASE_PATH: str = "metadata"
"""Directory for metadata storage."""

DATASET_IMAGES = "dataset_images"
"""Directory for dataset images."""

DEFAULT_DATA_DIR = "data"

DOCKERFILE = "Dockerfile"
"""Name of the Dockerfile in the repository."""

LOCK_SUFFIX = ".lock"
"""Default suffix for Renku lock file."""

POINTERS = "pointers"
"""Directory for storing external pointer files."""

REFS = "refs"
"""Define a name of the folder with references in the Renku folder."""

RENKU_LFS_IGNORE_PATH = ".renkulfsignore"
""".gitignore like file specifying paths that are not tracked in LFS."""

TEMPLATE_CHECKSUMS = "template_checksums.json"

RENKU_PROTECTED_PATHS = [
    ".dockerignore",
    ".git",
    ".git/*",
    ".gitattributes",
    ".gitignore",
    ".gitlab-ci.yml",
    ".renku",
    ".renku/**",
    ".renkulfsignore",
    "Dockerfile*",
    "environment.yml",
    "requirements.txt",
]

DATABASE_METADATA_PATH = [
    Path(RENKU_HOME) / DATABASE_PATH,
]

DATASET_METADATA_PATHS = [
    Path(RENKU_HOME) / DATABASE_PATH,
    Path(RENKU_HOME) / DATASET_IMAGES,
    Path(RENKU_HOME) / POINTERS,
    Path(RENKU_HOME) / REFS,
    ".gitattributes",
    ".gitignore",
]
