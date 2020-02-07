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

Changes
=======


`0.9.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.8.2...v0.9.0>`__ (2020-02-07)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  adds git user check before running renku init
   (`#892 <https://github.com/SwissDataScienceCenter/renku-python/issues/892>`__)
   (`2e52dff <https://github.com/SwissDataScienceCenter/renku-python/commit/2e52dff9dd627c93764aadb9fd1e91bd190a5de7>`__)
-  adds sorting to file listing
   (`#960 <https://github.com/SwissDataScienceCenter/renku-python/issues/960>`__)
   (`bcf6bcd <https://github.com/SwissDataScienceCenter/renku-python/commit/bcf6bcdeb55e6c6213319678696b1b5cb9646052>`__)
-  avoid empty commits when adding files
   (`#842 <https://github.com/SwissDataScienceCenter/renku-python/issues/842>`__)
   (`8533a7a <https://github.com/SwissDataScienceCenter/renku-python/commit/8533a7aa2e28e3937e194bef6a1d456e5dd5203e>`__)
-  Fixes dataset naming
   (`#898 <https://github.com/SwissDataScienceCenter/renku-python/issues/898>`__)
   (`418deb3 <https://github.com/SwissDataScienceCenter/renku-python/commit/418deb3d755d8b35ebf6d11184a2e8e2b9528aae>`__)
-  Deletes temporary branch after renku init –force
   (`#887 <https://github.com/SwissDataScienceCenter/renku-python/issues/887>`__)
   (`eac0463 <https://github.com/SwissDataScienceCenter/renku-python/commit/eac0463e31db29b7294e555bb17c47f24b02dd26>`__)
-  enforces label on SoftwareAgent
   (`#869 <https://github.com/SwissDataScienceCenter/renku-python/issues/869>`__)
   (`71badda <https://github.com/SwissDataScienceCenter/renku-python/commit/71badda7f70d847b0f938bba8d76f53b9f5c915e>`__)
-  Fixes JSON-LD translation and related issues
   (`#846 <https://github.com/SwissDataScienceCenter/renku-python/issues/846>`__)
   (`65e5469 <https://github.com/SwissDataScienceCenter/renku-python/commit/65e546915737bcf7ff1dd6648aad1e2dd963ca97>`__)
-  Fixes renku run error message handling
   (`#961 <https://github.com/SwissDataScienceCenter/renku-python/issues/961>`__)
   (`81d31ff <https://github.com/SwissDataScienceCenter/renku-python/commit/81d31ffca1fcb34fea009df0d55cf7d9b2ad60b5>`__)
-  Fixes renku update workflow failure handling and renku status error handling
   (`#888 <https://github.com/SwissDataScienceCenter/renku-python/issues/888>`__)
   (`3879124 <https://github.com/SwissDataScienceCenter/renku-python/commit/3879124a40fb17acc2cb46069598a63ae32b0075>`__)
-  Fixes sameAs property to follow schema.org spec
   (`#944 <https://github.com/SwissDataScienceCenter/renku-python/issues/944>`__)
   (`291380e <https://github.com/SwissDataScienceCenter/renku-python/commit/291380e16046484d789fbf6485cebe04ed6a61e2>`__)
-  handle missing renku directory
   (`#989 <https://github.com/SwissDataScienceCenter/renku-python/issues/989>`__)
   (`f938be9 <https://github.com/SwissDataScienceCenter/renku-python/commit/f938be9040049b17cb0f7d01f7c3681dc3221f13>`__)
-  resolves symlinks when pulling LFS
   (`#981 <https://github.com/SwissDataScienceCenter/renku-python/issues/981>`__)
   (`68bd8f5 <https://github.com/SwissDataScienceCenter/renku-python/commit/68bd8f5f5a60413529f9a5eeccfb7734b1cf8d32>`__)
-  serializes all zenodo metadata
   (`#941 <https://github.com/SwissDataScienceCenter/renku-python/issues/941>`__)
   (`787978a <https://github.com/SwissDataScienceCenter/renku-python/commit/787978ad787dc5682392545df0ac0024cf17837d>`__)
-  Fixes various bugs in dataset import
   (`#882 <https://github.com/SwissDataScienceCenter/renku-python/issues/882>`__)
   (`be28bf5 <https://github.com/SwissDataScienceCenter/renku-python/commit/be28bf5de72fac16e951b93fd95d3fe9b815f2d5>`__)

Features
~~~~~~~~

-  add project initialization from template
   (`#809 <https://github.com/SwissDataScienceCenter/renku-python/issues/809>`__)
   (`4405744 <https://github.com/SwissDataScienceCenter/renku-python/commit/440574404248518a34992ed2f3607e861983177d>`__)
-  added renku service with cache and datasets
   (`#788 <https://github.com/SwissDataScienceCenter/renku-python/issues/788>`__)
   (`7a7068d <https://github.com/SwissDataScienceCenter/renku-python/commit/7a7068d9912ad4112d85d32153ca06fc8386a8ce>`__),
   closes
   `#767 <https://github.com/SwissDataScienceCenter/renku-python/issues/767>`__
   `#846 <https://github.com/SwissDataScienceCenter/renku-python/issues/846>`__
-  Adds protection for renku relevant paths in dataset add
   (`#939 <https://github.com/SwissDataScienceCenter/renku-python/issues/939>`__)
   (`a3c02e8 <https://github.com/SwissDataScienceCenter/renku-python/commit/a3c02e897aa7cb891e16f530c76709114b97b105>`__)
-  Adds prov:Invalidated output to renku log
   (`008ab20 <https://github.com/SwissDataScienceCenter/renku-python/commit/008ab20fbd5e925231a92c58b628e821d0b43add>`__)
-  better UX when adding to a dataset
   (`#911 <https://github.com/SwissDataScienceCenter/renku-python/issues/911>`__)
   (`c6ac967 <https://github.com/SwissDataScienceCenter/renku-python/commit/c6ac9676c38caf96a70afbc1fddd08f9ec709238>`__)
-  check for required git hooks
   (`#854 <https://github.com/SwissDataScienceCenter/renku-python/issues/854>`__)
   (`54ba91d <https://github.com/SwissDataScienceCenter/renku-python/commit/54ba91d84f03668cb6f3dd29b2d1ca2b27b4346a>`__)
-  Dataverse export
   (`#909 <https://github.com/SwissDataScienceCenter/renku-python/issues/909>`__)
   (`7e9e647 <https://github.com/SwissDataScienceCenter/renku-python/commit/7e9e64798f1c7777a27e28c5600003407188a988>`__)
-  improve dataset command output
   (`#927 <https://github.com/SwissDataScienceCenter/renku-python/issues/927>`__)
   (`c7639d3 <https://github.com/SwissDataScienceCenter/renku-python/commit/c7639d3a359ab3b750ce5b8fb6d5e7f6dbd4374a>`__)
-  metadata on dataset creation
   (`#850 <https://github.com/SwissDataScienceCenter/renku-python/issues/850>`__)
   (`b357ee7 <https://github.com/SwissDataScienceCenter/renku-python/commit/b357ee71b081f0770d80a15e1704da634d7582c9>`__)
-  Plugin support for renku-run
   (`#883 <https://github.com/SwissDataScienceCenter/renku-python/issues/883>`__)
   (`7dbda83 <https://github.com/SwissDataScienceCenter/renku-python/commit/7dbda8383a1de260ff27d5863a38af792030adfd>`__)
-  python 3.8 compatibility
   (`#861 <https://github.com/SwissDataScienceCenter/renku-python/issues/861>`__)
   (`4aaac8d <https://github.com/SwissDataScienceCenter/renku-python/commit/4aaac8dfeecd648ccb946d339d9208bf13b18e1a>`__)
-  SHACL Validation
   (`#767 <https://github.com/SwissDataScienceCenter/renku-python/issues/767>`__)
   (`255a01d <https://github.com/SwissDataScienceCenter/renku-python/commit/255a01deff124266a5a89f756cb8f93c65153358>`__)
-  update bug_report template to be more renku-relevant
   (`#988 <https://github.com/SwissDataScienceCenter/renku-python/issues/988>`__)
   (`e00ded7 <https://github.com/SwissDataScienceCenter/renku-python/commit/e00ded783c05364f04a574b29284593860319bc1>`__)


`0.8.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.7.0...v0.8.0>`__ (2019-11-21)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  addressed CI problems with git submodules
   (`#783 <https://github.com/SwissDataScienceCenter/renku-python/issues/783>`__)
   (`0d3eeb7 <https://github.com/SwissDataScienceCenter/renku-python/commit/0d3eeb7>`__)
-  adds simple check on empty filename
   (`#786 <https://github.com/SwissDataScienceCenter/renku-python/issues/786>`__)
   (`8cd061b <https://github.com/SwissDataScienceCenter/renku-python/commit/8cd061b>`__)
-  ensure all Person instances have valid ids
   (`4f80efc <https://github.com/SwissDataScienceCenter/renku-python/commit/4f80efc>`__),
   closes
   `#812 <https://github.com/SwissDataScienceCenter/renku-python/issues/812>`__
-  Fixes jsonld issue when importing from dataverse
   (`#759 <https://github.com/SwissDataScienceCenter/renku-python/issues/759>`__)
   (`ffe36c6 <https://github.com/SwissDataScienceCenter/renku-python/commit/ffe36c6>`__)
-  fixes nested type scoped handling if a class only has a single class
   (`#804 <https://github.com/SwissDataScienceCenter/renku-python/issues/804>`__)
   (`16d03b6 <https://github.com/SwissDataScienceCenter/renku-python/commit/16d03b6>`__)
-  ignore deleted paths in generated entities
   (`86fedaf <https://github.com/SwissDataScienceCenter/renku-python/commit/86fedaf>`__),
   closes
   `#806 <https://github.com/SwissDataScienceCenter/renku-python/issues/806>`__
-  integration tests
   (`#831 <https://github.com/SwissDataScienceCenter/renku-python/issues/831>`__)
   (`a4ad7f9 <https://github.com/SwissDataScienceCenter/renku-python/commit/a4ad7f9>`__)
-  make Creator a subclass of Person
   (`ac9bac3 <https://github.com/SwissDataScienceCenter/renku-python/commit/ac9bac3>`__),
   closes
   `#793 <https://github.com/SwissDataScienceCenter/renku-python/issues/793>`__
-  Redesign scoped context in jsonld
   (`#750 <https://github.com/SwissDataScienceCenter/renku-python/issues/750>`__)
   (`2b1948d <https://github.com/SwissDataScienceCenter/renku-python/commit/2b1948d>`__)

Features
~~~~~~~~

-  avoid creation of nested datasets
   (`#796 <https://github.com/SwissDataScienceCenter/renku-python/issues/796>`__)
   (`6084c87 <https://github.com/SwissDataScienceCenter/renku-python/commit/6084c87>`__)
-  do not create dataset implicitly
   (`#779 <https://github.com/SwissDataScienceCenter/renku-python/issues/779>`__)
   (`84e59d0 <https://github.com/SwissDataScienceCenter/renku-python/commit/84e59d0>`__)
-  local git repo not treated as remote
   (`8cc2834 <https://github.com/SwissDataScienceCenter/renku-python/commit/8cc2834>`__)
-  renku clone command
   (`#828 <https://github.com/SwissDataScienceCenter/renku-python/issues/828>`__)
   (`4b3b615 <https://github.com/SwissDataScienceCenter/renku-python/commit/4b3b615>`__)


`0.7.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.6.0...v0.6.1>`__ (2019-10-15)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  use UI-resolved project path as project ID (`#701`_) (`dfcc9e6`_)


`0.6.1 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.6.0...v0.6.1>`__ (2019-10-10)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  add .renku/tmp to default .gitignore (`#728`_) (`6212148`_)
-  dataset import causes renku exception due to duplicate LocalClient
   (`#724`_) (`89411b0`_)
-  delete new dataset ref if file add fails (`#729`_) (`2dea711`_)
-  fixes bug with deleted files not getting committed (`#741`_)
   (`5de4b6f`_)
-  force current project for entities (`#707`_) (`538ef07`_)
-  integration tests for `#681`_ (`#747`_) (`b08435d`_)
-  use commit author for project creator (`#715`_) (`1a40ebe`_), closes
   `#713`_
-  zenodo dataset import error (`f1d623a`_)

Features
~~~~~~~~

-  adds basic QA checklist (`#698`_) (`c97e9bd`_)
-  dataset tagging (`#693`_) (`797161f`_)
-  include creator in project metadata (`#687`_) ([9c7753e](https://git

.. _0.7.0: https://github.com/SwissDataScienceCenter/renku-python/compare/v0.6.1...v
.. _#701: https://github.com/SwissDataScienceCenter/renku-python/issues/701
.. _dfcc9e6: https://github.com/SwissDataScienceCenter/renku-python/commit/dfcc9e6aa1899cf89d90223f2adc2b0dd8676ef4
.. _0.6.1: https://github.com/SwissDataScienceCenter/renku-python/compare/v0.6.0...v
.. _#728: https://github.com/SwissDataScienceCenter/renku-python/issues/728
.. _6212148: https://github.com/SwissDataScienceCenter/renku-python/commit/62121485119ed02a28a5b2e2990395bee284e348
.. _#724: https://github.com/SwissDataScienceCenter/renku-python/issues/724
.. _89411b0: https://github.com/SwissDataScienceCenter/renku-python/commit/89411b010a2671790aafccaa42216e5ccdf5a660
.. _#729: https://github.com/SwissDataScienceCenter/renku-python/issues/729
.. _2dea711: https://github.com/SwissDataScienceCenter/renku-python/commit/2dea711939a11c8c8c358bfd76499ae8865958ee
.. _#741: https://github.com/SwissDataScienceCenter/renku-python/issues/741
.. _5de4b6f: https://github.com/SwissDataScienceCenter/renku-python/commit/5de4b6feaa17d4db3fbec8e65523c28cfd80ca3e
.. _#707: https://github.com/SwissDataScienceCenter/renku-python/issues/707
.. _538ef07: https://github.com/SwissDataScienceCenter/renku-python/commit/538ef078b95c78d43907294147ca4fd359779b9e
.. _#681: https://github.com/SwissDataScienceCenter/renku-python/issues/681
.. _#747: https://github.com/SwissDataScienceCenter/renku-python/issues/747
.. _b08435d: https://github.com/SwissDataScienceCenter/renku-python/commit/b08435d26a19e69bff9a9b376526aee9ab3ecd70
.. _#715: https://github.com/SwissDataScienceCenter/renku-python/issues/715
.. _1a40ebe: https://github.com/SwissDataScienceCenter/renku-python/commit/1a40ebe75ea7c49cf52a9fce5c757d7352f159b0
.. _#713: https://github.com/SwissDataScienceCenter/renku-python/issues/713
.. _f1d623a: https://github.com/SwissDataScienceCenter/renku-python/commit/f1d623abf16287d62a3a0fecaf40e16d4ed5fc8b
.. _#698: https://github.com/SwissDataScienceCenter/renku-python/issues/698
.. _c97e9bd: https://github.com/SwissDataScienceCenter/renku-python/commit/c97e9bdc0c65fb6b06c7467624a988bf533f0b80
.. _#693: https://github.com/SwissDataScienceCenter/renku-python/issues/693
.. _797161f: https://github.com/SwissDataScienceCenter/renku-python/commit/797161fe48746137ae4c2761dfcbc7819124f154
.. _#687: https://github.com/SwissDataScienceCenter/renku-python/issues/687


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
