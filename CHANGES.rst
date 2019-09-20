..
    Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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

`0.6.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.2...v0.6.0>`__ (2019-09-18)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  adds \_label and commit data to imported dataset files, single commit
   for imports
   (`#651 <https://github.com/SwissDataScienceCenter/renku-python/issues/651>`__)
   (`75ce369 <https://github.com/SwissDataScienceCenter/renku-python/commit/75ce369>`__)
-  always add commit to dataset if possible
   (`#648 <https://github.com/SwissDataScienceCenter/renku-python/issues/648>`__)
   (`7659bc8 <https://github.com/SwissDataScienceCenter/renku-python/commit/7659bc8>`__),
   closes
   `#646 <https://github.com/SwissDataScienceCenter/renku-python/issues/646>`__
-  cleanup needed for integration tests on py35
   (`#653 <https://github.com/SwissDataScienceCenter/renku-python/issues/653>`__)
   (`fdd7215 <https://github.com/SwissDataScienceCenter/renku-python/commit/fdd7215>`__)
-  fixed serialization of datetime to iso format
   (`#629 <https://github.com/SwissDataScienceCenter/renku-python/issues/629>`__)
   (`693d59d <https://github.com/SwissDataScienceCenter/renku-python/commit/693d59d>`__)
-  fixes broken integration test
   (`#649 <https://github.com/SwissDataScienceCenter/renku-python/issues/649>`__)
   (`04eba66 <https://github.com/SwissDataScienceCenter/renku-python/commit/04eba66>`__)
-  hide image, pull, runner, show, workon and deactivate commands
   (`#672 <https://github.com/SwissDataScienceCenter/renku-python/issues/672>`__)
   (`a3e9998 <https://github.com/SwissDataScienceCenter/renku-python/commit/a3e9998>`__)
-  integration tests fixed
   (`#685 <https://github.com/SwissDataScienceCenter/renku-python/issues/685>`__)
   (`f0ea8f0 <https://github.com/SwissDataScienceCenter/renku-python/commit/f0ea8f0>`__)
-  migration of old datasets
   (`#639 <https://github.com/SwissDataScienceCenter/renku-python/issues/639>`__)
   (`4d4d7d2 <https://github.com/SwissDataScienceCenter/renku-python/commit/4d4d7d2>`__)
-  migration timezones
   (`#683 <https://github.com/SwissDataScienceCenter/renku-python/issues/683>`__)
   (`58c2de4 <https://github.com/SwissDataScienceCenter/renku-python/commit/58c2de4>`__)
-  Removes unneccesary call to git lfs with no paths
   (`#658 <https://github.com/SwissDataScienceCenter/renku-python/issues/658>`__)
   (`e32d48b <https://github.com/SwissDataScienceCenter/renku-python/commit/e32d48b>`__)
-  renku home directory overwrite in tests
   (`#657 <https://github.com/SwissDataScienceCenter/renku-python/issues/657>`__)
   (`90e1c48 <https://github.com/SwissDataScienceCenter/renku-python/commit/90e1c48>`__)
-  upload metadata before actual files
   (`#652 <https://github.com/SwissDataScienceCenter/renku-python/issues/652>`__)
   (`95ed468 <https://github.com/SwissDataScienceCenter/renku-python/commit/95ed468>`__)
-  use latest_html for version check
   (`#647 <https://github.com/SwissDataScienceCenter/renku-python/issues/647>`__)
   (`c6b0309 <https://github.com/SwissDataScienceCenter/renku-python/commit/c6b0309>`__),
   closes
   `#641 <https://github.com/SwissDataScienceCenter/renku-python/issues/641>`__
-  user-related metadata
   (`#655 <https://github.com/SwissDataScienceCenter/renku-python/issues/655>`__)
   (`44183e6 <https://github.com/SwissDataScienceCenter/renku-python/commit/44183e6>`__)
-  zenodo export failing with relative paths
   (`d40967c <https://github.com/SwissDataScienceCenter/renku-python/commit/d40967c>`__)

Features
~~~~~~~~

-  dataverse import
   (`#626 <https://github.com/SwissDataScienceCenter/renku-python/issues/626>`__)
   (`9f0f9a1 <https://github.com/SwissDataScienceCenter/renku-python/commit/9f0f9a1>`__)
-  enable all datasets command to operate on dirty repository
   (`#607 <https://github.com/SwissDataScienceCenter/renku-python/issues/607>`__)
   (`74e328b <https://github.com/SwissDataScienceCenter/renku-python/commit/74e328b>`__)
-  explicit input output specification
   (`#598 <https://github.com/SwissDataScienceCenter/renku-python/issues/598>`__)
   (`ce8ba67 <https://github.com/SwissDataScienceCenter/renku-python/commit/ce8ba67>`__)
-  export filename as schema:name
   (`#643 <https://github.com/SwissDataScienceCenter/renku-python/issues/643>`__)
   (`aed54bf <https://github.com/SwissDataScienceCenter/renku-python/commit/aed54bf>`__),
   closes
   `#640 <https://github.com/SwissDataScienceCenter/renku-python/issues/640>`__
-  support for indirect inputs and outputs
   (`#650 <https://github.com/SwissDataScienceCenter/renku-python/issues/650>`__)
   (`e960a98 <https://github.com/SwissDataScienceCenter/renku-python/commit/e960a98>`__)


`0.5.2 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.1...v0.5.2>`__ (2019-07-26)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  safe_path check always operates on str
   (`#603 <https://github.com/SwissDataScienceCenter/renku-python/issues/603>`__)
   (`7c1c34e <https://github.com/SwissDataScienceCenter/renku-python/commit/7c1c34e>`__)

Features
~~~~~~~~

-  add SoftwareAgent to Activity
   (`#590 <https://github.com/SwissDataScienceCenter/renku-python/issues/590>`__)
   (`a60c20c <https://github.com/SwissDataScienceCenter/renku-python/commit/a60c20c>`__),
   closes
   `#508 <https://github.com/SwissDataScienceCenter/renku-python/issues/508>`__


`0.5.1 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.0...v0.5.1>`__ (2019-07-12)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  ensure external storage is handled correctly
   (`#592 <https://github.com/SwissDataScienceCenter/renku-python/issues/592>`__)
   (`7938ac4 <https://github.com/SwissDataScienceCenter/renku-python/commit/7938ac4>`__)
-  only check local repo for lfs filter
   (`#575 <https://github.com/SwissDataScienceCenter/renku-python/issues/575>`__)
   (`a64dc79 <https://github.com/SwissDataScienceCenter/renku-python/commit/a64dc79>`__)
-  **cli:** allow renku run with many inputs
   (`f60783e <https://github.com/SwissDataScienceCenter/renku-python/commit/f60783e>`__),
   closes
   `#552 <https://github.com/SwissDataScienceCenter/renku-python/issues/552>`__
-  added check for overwriting datasets
   (`#541 <https://github.com/SwissDataScienceCenter/renku-python/issues/541>`__)
   (`8c697fb <https://github.com/SwissDataScienceCenter/renku-python/commit/8c697fb>`__)
-  escape whitespaces in notebook name
   (`#584 <https://github.com/SwissDataScienceCenter/renku-python/issues/584>`__)
   (`0542fcc <https://github.com/SwissDataScienceCenter/renku-python/commit/0542fcc>`__)
-  modify json-ld for datasets
   (`#534 <https://github.com/SwissDataScienceCenter/renku-python/issues/534>`__)
   (`ab6a719 <https://github.com/SwissDataScienceCenter/renku-python/commit/ab6a719>`__),
   closes
   `#525 <https://github.com/SwissDataScienceCenter/renku-python/issues/525>`__
   `#526 <https://github.com/SwissDataScienceCenter/renku-python/issues/526>`__
-  refactored tests and docs to align with updated pydoctstyle
   (`#586 <https://github.com/SwissDataScienceCenter/renku-python/issues/586>`__)
   (`6f981c8 <https://github.com/SwissDataScienceCenter/renku-python/commit/6f981c8>`__)
-  **cli:** add check of missing references
   (`9a373da <https://github.com/SwissDataScienceCenter/renku-python/commit/9a373da>`__)
-  **cli:** fail when removing non existing dataset
   (`dd728db <https://github.com/SwissDataScienceCenter/renku-python/commit/dd728db>`__)
-  **status:** fix renku status output when not in root folder
   (`#564 <https://github.com/SwissDataScienceCenter/renku-python/issues/564>`__)
   (`873270d <https://github.com/SwissDataScienceCenter/renku-python/commit/873270d>`__),
   closes
   `#551 <https://github.com/SwissDataScienceCenter/renku-python/issues/551>`__
-  added dependencies for SSL support
   (`#565 <https://github.com/SwissDataScienceCenter/renku-python/issues/565>`__)
   (`4fa0fed <https://github.com/SwissDataScienceCenter/renku-python/commit/4fa0fed>`__)
-  **datasets:** strip query string from data filenames
   (`450898b <https://github.com/SwissDataScienceCenter/renku-python/commit/450898b>`__)
-  fixed serialization of creators
   (`#550 <https://github.com/SwissDataScienceCenter/renku-python/issues/550>`__)
   (`6a9173c <https://github.com/SwissDataScienceCenter/renku-python/commit/6a9173c>`__)
-  updated docs
   (`#539 <https://github.com/SwissDataScienceCenter/renku-python/issues/539>`__)
   (`ff9a67c <https://github.com/SwissDataScienceCenter/renku-python/commit/ff9a67c>`__)
-  **cli:** remove dataset aliases
   (`6206e62 <https://github.com/SwissDataScienceCenter/renku-python/commit/6206e62>`__)
-  **cwl:** detect script as input parameter
   (`e23b75a <https://github.com/SwissDataScienceCenter/renku-python/commit/e23b75a>`__),
   closes
   `#495 <https://github.com/SwissDataScienceCenter/renku-python/issues/495>`__
-  **deps:** updated dependencies
   (`691644d <https://github.com/SwissDataScienceCenter/renku-python/commit/691644d>`__)


Features
~~~~~~~~

-  add dataset metadata to the KG
   (`#558 <https://github.com/SwissDataScienceCenter/renku-python/issues/558>`__)
   (`fb443d7 <https://github.com/SwissDataScienceCenter/renku-python/commit/fb443d7>`__)
-  **datasets:** export dataset to zenodo
   (`#529 <https://github.com/SwissDataScienceCenter/renku-python/issues/529>`__)
   (`fc6fd4f <https://github.com/SwissDataScienceCenter/renku-python/commit/fc6fd4f>`__)
-  added support for working on dirty repo
   (`ae67be7 <https://github.com/SwissDataScienceCenter/renku-python/commit/ae67be7>`__)
-  **datasets:** edit dataset metadata
   (`#549 <https://github.com/SwissDataScienceCenter/renku-python/issues/549>`__)
   (`db39083 <https://github.com/SwissDataScienceCenter/renku-python/commit/db39083>`__)
-  integrate metadata from zenodo
   (`#545 <https://github.com/SwissDataScienceCenter/renku-python/issues/545>`__)
   (`4273d2a <https://github.com/SwissDataScienceCenter/renku-python/commit/4273d2a>`__)
-  **config:** added global config manager
   (`#533 <https://github.com/SwissDataScienceCenter/renku-python/issues/533>`__)
   (`938f820 <https://github.com/SwissDataScienceCenter/renku-python/commit/938f820>`__)
-  **datasets:** import data from zenodo
   (`#509 <https://github.com/SwissDataScienceCenter/renku-python/issues/509>`__)
   (`52b2769 <https://github.com/SwissDataScienceCenter/renku-python/commit/52b2769>`__)


`0.5.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.4.0...v0.5.0>`__ (2019-03-28)
-------------------------------------------------------------------------------------------------------


Bug Fixes
~~~~~~~~~

-  **api:** make methods lock free
   (`1f63964 <https://github.com/SwissDataScienceCenter/renku-python/commit/1f63964>`__),
   closes
   `#486 <https://github.com/SwissDataScienceCenter/renku-python/issues/486>`__
-  use safe_load for parsing yaml
   (`5383d1e <https://github.com/SwissDataScienceCenter/renku-python/commit/5383d1e>`__),
   closes
   `#464 <https://github.com/SwissDataScienceCenter/renku-python/issues/464>`__
-  **datasets:** link flag on dataset add
   (`eae30f4 <https://github.com/SwissDataScienceCenter/renku-python/commit/eae30f4>`__)


Features
~~~~~~~~

-  **api:** list datasets from a commit
   (`04a9fe9 <https://github.com/SwissDataScienceCenter/renku-python/commit/04a9fe9>`__)
-  **cli:** add dataset rm command
   (`a70c7ce <https://github.com/SwissDataScienceCenter/renku-python/commit/a70c7ce>`__)
-  **cli:** add rm command
   (`cf0f502 <https://github.com/SwissDataScienceCenter/renku-python/commit/cf0f502>`__)
-  **cli:** configurable format of dataset output
   (`d37abf3 <https://github.com/SwissDataScienceCenter/renku-python/commit/d37abf3>`__)
-  **dataset:** add existing file from current repo
   (`575686b <https://github.com/SwissDataScienceCenter/renku-python/commit/575686b>`__),
   closes
   `#99 <https://github.com/SwissDataScienceCenter/renku-python/issues/99>`__
-  **datasets:** added ls-files command
   (`ccc4f59 <https://github.com/SwissDataScienceCenter/renku-python/commit/ccc4f59>`__)
-  **models:** reference context for relative paths
   (`5d1e8e7 <https://github.com/SwissDataScienceCenter/renku-python/commit/5d1e8e7>`__),
   closes
   `#452 <https://github.com/SwissDataScienceCenter/renku-python/issues/452>`__
-  add JSON-LD output format for datasets
   (`c755d7b <https://github.com/SwissDataScienceCenter/renku-python/commit/c755d7b>`__),
   closes
   `#426 <https://github.com/SwissDataScienceCenter/renku-python/issues/426>`__
-  generate Makefile with log –format Makefile
   (`1e440ce <https://github.com/SwissDataScienceCenter/renku-python/commit/1e440ce>`__)

``v0.4.0``
----------

*(released 2019-03-05)*

- Adds ``renku mv`` command which updates dataset metadata, ``.gitattributes``
  and symlinks.
- Pulls LFS objects from submodules correctly.
- Adds listing of datasets.
- Adds reduced dot format for ``renku log``.
- Adds ``doctor`` command to check missing files in datasets.
- Moves dataset metadata to ``.renku/datasets`` and adds ``migrate datasets``
  command and uses UUID for metadata path.
- Gets git attrs for files to prevent duplicates in ``.gitattributes``.
- Fixes ``renku show outputs`` for directories.
- Runs Git LFS checkout in a worktrees and lazily pulls necessary LFS files
  before running commands.
- Asks user before overriding an existing file using ``renku init``
  or ``renku runner template``.
- Fixes ``renku init --force`` in an empty dir.
- Renames ``CommitMixin._location`` to ``_project``.
- Addresses issue with commits editing multiple CWL files.
- Exports merge commits for full lineage.
- Exports path and parent directories.
- Adds an automatic check for the latest version.
- Simplifies issue submission from traceback to GitHub or Sentry.
  Requires ``SENTRY_DSN`` variable to be set and `sentry-sdk` package to be
  installed before sending any data.
- Removes outputs before run.
- Allows update of directories.
- Improves readability of the status message.
- Checks ignored path when added to a dataset.
- Adds API method for finding ignored paths.
- Uses branches for ``init --force``.
- Fixes CVE-2017-18342.
- Fixes regex for parsing Git remote URLs.
- Handles ``--isolation`` option using ``git worktree``.
- Renames ``client.git`` to ``client.repo``.
- Supports ``python -m renku``.
- Allows '.' and '-' in repo path.


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
