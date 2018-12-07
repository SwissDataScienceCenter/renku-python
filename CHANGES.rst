..
    Copyright 2017-2018 - Swiss Data Science Center (SDSC)
    A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
    Eidgenössische Technische Hochschule Zürich (ETHZ).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

Changes
=======

``v0.3.3``
----------

*(released 2018-12-07)*

- Fixes generated Homebrew formula.
- Renames ``renku pull path`` to ``renku storage pull`` with deprecation
  warning.

``v0.3.2``
----------

*(released 2018-11-29)*

- Fixes display of workflows in ``renku log``.

``v0.3.1``
----------

*(released 2018-11-29)*

- Fixes issues with parsing remote Git URLs.

``v0.3.0``
----------

*(released 2018-11-26)*

- Adds JSON-LD context to objects extracted from the Git repository
  (see ``renku show context --list``).
- Uses PROV-O and WFPROV as provenance vocabularies and generates "stable"
  object identifiers (``@id``) for RDF and JSON-LD output formats.
- Refactors the log output to allow linking files and directories.
- Adds support for aliasing tools and workflows.
- Adds option to install shell completion (``renku --install-completion``).
- Fixes initialization of Git submodules.
- Uses relative submodule paths when appropriate.
- Simplifies external storage configuration.

``v0.2.0``
----------

*(released 2018-09-25)*

- Refactored version using Git and Common Workflow Language.

``v0.1.0``
----------

*(released 2017-09-06)*

- Initial public release as Renga.
