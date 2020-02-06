..
    Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

==============================
 Renku Python QA Protocol
==============================

This document describes smoke tests for checking that a new ``renku-python``
release works with basic functionality.

Setup & general
---------------

- [ ] Check installation via pip.
- [ ] Check output of ``renku --version``.
- [ ] Check that integration tests run successfully.
- [ ] Look at the docs and make sure they're up to date.
- [ ] Check that ``--help`` is up to date for changed commands.

Test new repository
-------------------
- [ ] Init renku in a bare repo.
- [ ] Create a dataset and add a file.
- [ ] List datasets and files.
- [ ] Import a dataset from Zenodo and/or Dataverse.
- [ ] List datasets and files.
- [ ] Remove a dataset.
- [ ] Create a workflow process with at least two steps, which counts the
  words in a file and outputs the result into another file, utilizing
  `renku run`.
- [ ] Change the input file from the previous step and rerun the workflow
  to check the new results.
- [ ] Check that renku log looks sensible.
- [ ] Check that git log looks sensible.

Test existing repository
------------------------
- [ ] Enter an existing repository.
- [ ] Run ``renku migrate`` commands.
- [ ] List datasets and dataset files.
- [ ] Rerun an existing workflow.
- [ ] Check that renku log looks sensible.
- [ ] Check that git log looks sensible.
