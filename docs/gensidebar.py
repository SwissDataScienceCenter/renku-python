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
"""Sidebar configuration."""

import os


def write_if_changed(fname, contents):
    """Overwrite file if contents have changed."""
    try:
        with open(fname, "r") as fp:
            old_contents = fp.read()
    except FileNotFoundError:
        old_contents = ""

    if old_contents != contents:
        with open(fname, "w") as fp:
            fp.write(contents)


def generate_sidebar(on_rtd, rtd_version, conf_api):
    """Generate the side bar."""
    do_gen = os.environ.get("SIDEBAR", None) == "1" or on_rtd

    lines = ["", ".. DO NOT MODIFY! THIS PAGE IS AUTOGENERATED!", ""]

    def toctree(name, max_depth=2, hidden=False, include_hidden=False):
        """Add a toctree entry."""
        lines.append(".. toctree::")

        if name:
            lines.append(f"    :caption: {name}")
        if hidden:
            lines.append("    :hidden:")
        if include_hidden:
            lines.append("    :includehidden:")
        lines.extend([f"    :maxdepth: {max_depth}", ""])

    def endl():
        """End a toctree."""
        lines.append("")

    def write(desc, link, repo):
        """Write a menu entry."""
        if conf_api == repo:
            args = desc, link
        elif not do_gen:
            return
        else:
            args = (
                desc,
                f"https://{repo}.readthedocs.io/en/{rtd_version}/{link}.html",
            )

        lines.append("    {} <{}>".format(*args))

    def write_external(desc, link):
        """Write a menu entry."""
        lines.append(f"    {desc} <{link}>")

    #
    # Specify the sidebar contents here
    #

    toctree(None, hidden=True)
    write("About Renku", "introduction/index", "renku")
    write("Getting Started", "getting_started", "renku")
    write("Using Renku", "user/index", "renku")
    write("Renku Platform", "platform", "renku")

    toctree(None, max_depth=3, hidden=True, include_hidden=False)
    write("Renku Client", "introduction", "renku-python")
    write("Renku Service", "service", "renku-python")

    toctree(None, max_depth=1, hidden=True)
    write("Get in touch", "get_in_touch", "renku")
    write("Release Notes", "release_notes", "renku")
    write_external("License", "https://www.apache.org/licenses/LICENSE-2.0")
    endl()

    write_if_changed("_sidebar.rst.inc", "\n".join(lines))
