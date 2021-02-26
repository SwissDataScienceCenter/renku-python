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
"""Show provenance of data created by executing programs.

File provenance
~~~~~~~~~~~~~~~

Unlike the traditional file history format, which shows previous revisions
of the file, this format presents tool inputs together with their revision
identifiers.

A ``*`` character shows to which lineage the specific file belongs to.
A ``@`` character in the graph lineage means that the corresponding file does
not have any inputs and the history starts there.

When called without file names, ``renku log`` shows the history of most
recently created files. With the ``--revision <refname>`` option the output is
shown as it was in the specified revision.

Provenance examples
```````````````````

``renku log B``
   Show the history of file ``B`` since its last creation or modification.

``renku log --revision HEAD~5``
   Show the history of files that have been created or modified 5 commits ago.

``renku log --revision e3f0bd5a D E``
   Show the history of files ``D`` and ``E`` as it looked in the commit
   ``e3f0bd5a``.

Output formats
~~~~~~~~~~~~~~

Following formats supported when specified with ``--format`` option:

* `ascii`
* `dot`
* `dot-full`
* `dot-landscape`
* `dot-full-landscape`
* `dot-debug`
* `json-ld`
* `json-ld-graph`
* `Makefile`
* `nt`
* `rdf`

You can generate a PNG of the full history of all files in the repository
using the :program:`dot` program.

.. code-block:: console

   $ FILES=$(git ls-files --no-empty-directory --recurse-submodules)
   $ renku log --format dot $FILES | dot -Tpng > /tmp/graph.png
   $ open /tmp/graph.png

Output validation
~~~~~~~~~~~~~~~~~

The ``--strict`` option forces the output to be validated against the Renku
SHACL schema, causing the command to fail if the generated output is not
valid, as well as printing detailed information on all the issues found.
The ``--strict`` option is only supported for the ``jsonld``, ``rdf`` and
``nt`` output formats.

"""

import click

from renku.core.commands.format.graph import FORMATS
from renku.core.commands.graph import build_graph


@click.command()
@click.option("--revision", default="HEAD", help="The git revision to generate the log for, default: HEAD")
@click.option("--format", type=click.Choice(FORMATS), default="ascii", help="Choose an output format.")
@click.option("--no-output", is_flag=True, default=False, help="Display commands without output files.")
@click.option("--strict", is_flag=True, default=False, help="Validate triples before output.")
@click.argument("paths", type=click.Path(exists=False), nargs=-1)
def log(revision, format, no_output, strict, paths):
    """Show logs for a file."""
    graph = build_graph(revision, no_output, paths)
    FORMATS[format](graph, strict=strict)
