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
"""Build script executed by Poetry."""

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

URL = "https://github.com/SwissDataScienceCenter/renku-project-template"
REFERENCE = "0.2.1"


def build() -> None:
    from git import Repo

    with TemporaryDirectory() as tempdir:
        # download and extract template data
        print("downloading Renku templates...")
        repo = Repo.clone_from(URL, tempdir)

        repo.git.checkout(REFERENCE)

        # copy templates
        current_path = Path.cwd()
        template_path = current_path / "renku" / "templates"
        build_path = current_path / "build" / "lib" / "renku" / "templates"
        if template_path.exists():
            shutil.rmtree(str(template_path))
        if build_path.exists():
            shutil.rmtree(str(build_path))
        shutil.copytree(tempdir, str(template_path), ignore=shutil.ignore_patterns(".git"))
        shutil.copytree(tempdir, str(build_path), ignore=shutil.ignore_patterns(".git"))


if __name__ == "__main__":
    build()
