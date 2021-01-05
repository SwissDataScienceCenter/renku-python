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

` <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.12.2...v0.12.3>`__ (2021-01-05)
----------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** fix gitlab ID parsing when GITLAB_BASE_URL is set without
   port
   (`#1823 <https://github.com/SwissDataScienceCenter/renku-python/pull/1823>`__)
   (`4f94165 <https://github.com/SwissDataScienceCenter/renku-python/commit/4f94165bbc84b1afd01e1ffcd6cf8a2a9ea25fb3>`__)
-  **service:** add datasets.remove to swagger docs
   (`#1778 <https://github.com/SwissDataScienceCenter/renku-python/issues/1778>`__)
   (`631e6f5 <https://github.com/SwissDataScienceCenter/renku-python/commit/631e6f594c87f028d4f904ee74310f252db49ce7>`__)
-  **service:** correctly handle cloning of project with no commits
   (`#1790 <https://github.com/SwissDataScienceCenter/renku-python/issues/1790>`__)
   (`440b238 <https://github.com/SwissDataScienceCenter/renku-python/commit/440b238ccb87f034f0cc6f1ea57b1f46d5213750>`__)

` <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.12.1...v0.12.2>`__ (2020-12-02)
----------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** correctly generate project id for gitlab (sub)groups
   (`#1746 <https://github.com/SwissDataScienceCenter/renku-python/issues/1746>`__)
   (`3fc29ad <https://github.com/SwissDataScienceCenter/renku-python/commit/3fc29ad8c754e060cd9b344fa6f11331ad59b23e>`__)
-  **core:** fixes renku save to work with already staged changes
   (`#1739 <https://github.com/SwissDataScienceCenter/renku-python/issues/1739>`__)
   (`1a8b7ad <https://github.com/SwissDataScienceCenter/renku-python/commit/1a8b7adf9d30a44c87f7ef8127df845a9f9f41fd>`__)
-  **core:** adds pre-commit hook message for unsupported projects
   (`#1730 <https://github.com/SwissDataScienceCenter/renku-python/issues/1730>`__)
   (`7f1731d <https://github.com/SwissDataScienceCenter/renku-python/commit/7f1731de7661ee05178fce54606079941fe67dc8>`__)
-  **service:** removes chdir calls in service
   (`#1767 <https://github.com/SwissDataScienceCenter/renku-python/issues/1767>`__)
   (`4da22cb <https://github.com/SwissDataScienceCenter/renku-python/commit/4da22cb8b88621df5087640c8aca68e48a56a0e1>`__)

Features
~~~~~~~~

-  **api:** adds user-api parameters support
   (`#1723 <https://github.com/SwissDataScienceCenter/renku-python/issues/1723>`__)
   (`6ee2862 <https://github.com/SwissDataScienceCenter/renku-python/commit/6ee286241f689d781c67b65e4b45297f5fe2fa59>`__)
-  **cli:** adds migrationscheck command
   (`#1761 <https://github.com/SwissDataScienceCenter/renku-python/issues/1761>`__)
   (`b33ed35 <https://github.com/SwissDataScienceCenter/renku-python/commit/b33ed35ef6bf52c73b1c689e907feffcd80608c3>`__)
-  **cli:** automatically track files in git-lfs if necessary
   (`#1775 <https://github.com/SwissDataScienceCenter/renku-python/issues/1775>`__)
   (`866163a <https://github.com/SwissDataScienceCenter/renku-python/commit/866163a7ff12afae51f9cf3fe0ec7fad9d40d26b>`__)
-  **cli:** better error messages for renku clone
   (`#1738 <https://github.com/SwissDataScienceCenter/renku-python/issues/1738>`__)
   (`78bb2ad <https://github.com/SwissDataScienceCenter/renku-python/commit/78bb2ad5b3f0edb0f030e76e74b5e45cfea8de89>`__)
-  **core:** shorten commit messages to 100 characters for readability
   (`#1749 <https://github.com/SwissDataScienceCenter/renku-python/issues/1749>`__)
   (`af50947 <https://github.com/SwissDataScienceCenter/renku-python/commit/af50947a1e1efaf4dd12a4f589a07c086505e5b1>`__)
-  **service:** move user identification to jwt
   (`#1520 <https://github.com/SwissDataScienceCenter/renku-python/issues/1520>`__)
   (`d45c4c3 <https://github.com/SwissDataScienceCenter/renku-python/commit/d45c4c3062fc36abc650160d48a3bccde38cea84>`__)

` <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.12.0...v0.12.1>`__ (2020-11-16)
----------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** re-raise renku handled exception on network failure
   (`#1623 <https://github.com/SwissDataScienceCenter/renku-python/issues/1623>`__)
   (`4856a05 <https://github.com/SwissDataScienceCenter/renku-python/commit/4856a05fa15fa10f402fc95289a641c1f52617bc>`__)
-  **dataset:** no commit if nothing is edited
   (`#1706 <https://github.com/SwissDataScienceCenter/renku-python/issues/1706>`__)
   (`a68edf6 <https://github.com/SwissDataScienceCenter/renku-python/commit/a68edf6c9d654e3fcf9203c530e9c7fdae256f76>`__)
-  **service:** correctly determine resource age
   (`#1695 <https://github.com/SwissDataScienceCenter/renku-python/issues/1695>`__)
   (`40153f0 <https://github.com/SwissDataScienceCenter/renku-python/commit/40153f099ce684e96ffaf2502771c185921d0a96>`__)
-  **service:** correctly set project_name slug on project create
   (`#1691 <https://github.com/SwissDataScienceCenter/renku-python/issues/1691>`__)
   (`234e1b3 <https://github.com/SwissDataScienceCenter/renku-python/commit/234e1b376727bffaad1ea8dc2a06e607bdebeb5c>`__)
-  **service:** set template version and metadata correctly
   (`#1708 <https://github.com/SwissDataScienceCenter/renku-python/issues/1708>`__)
   (`ed98be3 <https://github.com/SwissDataScienceCenter/renku-python/commit/ed98be377d7ff405768ec2fbb44cd3d60949b9aa>`__)

Features
~~~~~~~~

-  renku API
   (`#1665 <https://github.com/SwissDataScienceCenter/renku-python/issues/1665>`__)
   (`949a4aa <https://github.com/SwissDataScienceCenter/renku-python/commit/949a4aa46ef8b5a3c331ae34a6250e8d5cb433d2>`__)

` <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.6...v0.12.0>`__ (2020-11-03)
----------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** fix bug where remote_cache caused project ids to leak
   (`#1618 <https://github.com/SwissDataScienceCenter/renku-python/issues/1618>`__)
   (`3ef04fb <https://github.com/SwissDataScienceCenter/renku-python/commit/3ef04fb8b6091d87b235901f7dfa22728f67b540>`__)
-  **core:** fix graph building for nodes with same subpath
   (`#1625 <https://github.com/SwissDataScienceCenter/renku-python/issues/1625>`__)
   (`7cae9be <https://github.com/SwissDataScienceCenter/renku-python/commit/7cae9be483a3ec4d1d68de2c6cc9251ee8c6a9a8>`__)
-  **core:** fix importing a dataset referenced from non-existent
   projects
   (`#1574 <https://github.com/SwissDataScienceCenter/renku-python/issues/1574>`__)
   (`92b8bf8 <https://github.com/SwissDataScienceCenter/renku-python/commit/92b8bf8b2f0727d8fe5a7ea601123e70568bc94d>`__)
-  **core:** fix old dataset migration and activity dataset outputs
   (`#1603 <https://github.com/SwissDataScienceCenter/renku-python/issues/1603>`__)
   (`a5339e2 <https://github.com/SwissDataScienceCenter/renku-python/commit/a5339e219e9cdf606f32bf2dd78e28be5ddd6f11>`__)
-  **core:** fix project migration getting overwritten with old metadata
   (`#1581 <https://github.com/SwissDataScienceCenter/renku-python/issues/1581>`__)
   (`c5a5960 <https://github.com/SwissDataScienceCenter/renku-python/commit/c5a5960e3e7eba908acef90701b9b4e296ca860a>`__)
-  **core:** fix update creating a commit when showing help
   (`#1627 <https://github.com/SwissDataScienceCenter/renku-python/issues/1627>`__)
   (`529e582 <https://github.com/SwissDataScienceCenter/renku-python/commit/529e5825647a319c7dd1b468301f961191fef6d3>`__)
-  **core:** fixes git encoding of paths with unicode characters
   (`#1538 <https://github.com/SwissDataScienceCenter/renku-python/issues/1538>`__)
   (`053dac9 <https://github.com/SwissDataScienceCenter/renku-python/commit/053dac99a1f0f99b05a5aff4fbeb6175f277565b>`__)
-  **core:** make Run migration ids unique by relative path instead of
   absolute
   (`#1573 <https://github.com/SwissDataScienceCenter/renku-python/issues/1573>`__)
   (`cf96310 <https://github.com/SwissDataScienceCenter/renku-python/commit/cf96310d68eb94dc931820d602ce81df7086bd00>`__)
-  **dataset:** broken directory hierarchy after renku dataset imports
   (`#1576 <https://github.com/SwissDataScienceCenter/renku-python/issues/1576>`__)
   (`9dcffce <https://github.com/SwissDataScienceCenter/renku-python/commit/9dcffce18dd81f9520a2bc5aa84c5d989537ccf9>`__)
-  **dataset:** deserialization error
   (`#1675 <https://github.com/SwissDataScienceCenter/renku-python/issues/1675>`__)
   (`420653f <https://github.com/SwissDataScienceCenter/renku-python/commit/420653f3c7b6f53fb22eda6fa2ded3333801dcf0>`__)
-  **dataset:** error when adding same file multiple times
   (`#1639 <https://github.com/SwissDataScienceCenter/renku-python/issues/1639>`__)
   (`05bfde7 <https://github.com/SwissDataScienceCenter/renku-python/commit/05bfde79ada00c1dca1def3530419683ceea0820>`__)
-  **dataset:** explicit failure when cannot pull LFS objects
   (`#1590 <https://github.com/SwissDataScienceCenter/renku-python/issues/1590>`__)
   (`3b05816 <https://github.com/SwissDataScienceCenter/renku-python/commit/3b05816d86b32326ef06d21752fb7493b0c60af9>`__)
-  **dataset:** invalid generated name in migration
   (`#1593 <https://github.com/SwissDataScienceCenter/renku-python/issues/1593>`__)
   (`89b2e43 <https://github.com/SwissDataScienceCenter/renku-python/commit/89b2e43df7122d4fc31c359d86b874fb3cccdad2>`__)
-  **dataset:** remove blank nodes
   (`#1602 <https://github.com/SwissDataScienceCenter/renku-python/issues/1602>`__)
   (`478f08c <https://github.com/SwissDataScienceCenter/renku-python/commit/478f08c4388f65a4369c7f5119897ee060cece26>`__)
-  **dataset:** set isBasedOn for renku datasets
   (`#1617 <https://github.com/SwissDataScienceCenter/renku-python/issues/1617>`__)
   (`3aee6b8 <https://github.com/SwissDataScienceCenter/renku-python/commit/3aee6b84c673ee5005a642168556c10e5ea4bd4f>`__)
-  **dataset:** update local files metadata when overwriting
   (`#1582 <https://github.com/SwissDataScienceCenter/renku-python/issues/1582>`__)
   (`59eaf25 <https://github.com/SwissDataScienceCenter/renku-python/commit/59eaf25e37c737a2adfffbb3890a545b6e88b496>`__)
-  **dataset:** various migration issues
   (`#1620 <https://github.com/SwissDataScienceCenter/renku-python/issues/1620>`__)
   (`f24c2e4 <https://github.com/SwissDataScienceCenter/renku-python/commit/f24c2e4003d6bd2b53fb19e3494cce3651957921>`__)
-  **service:** correctely set job timeout
   (`#1677 <https://github.com/SwissDataScienceCenter/renku-python/issues/1677>`__)
   (`25f0eb6 <https://github.com/SwissDataScienceCenter/renku-python/commit/25f0eb65e31ed60f09825972f8172338fbf302bf>`__)
-  **service:** dataset rm endpoint supports new core API
   (`#1622 <https://github.com/SwissDataScienceCenter/renku-python/issues/1622>`__)
   (`e71916e <https://github.com/SwissDataScienceCenter/renku-python/commit/e71916ef2d75ee2ef34f43301dea3da790f53107>`__)
-  **service:** push to protected branches
   (`#1614 <https://github.com/SwissDataScienceCenter/renku-python/issues/1614>`__)
   (`34c7f92 <https://github.com/SwissDataScienceCenter/renku-python/commit/34c7f921ea04bfbfdbf08c85da165414efc315a6>`__)
-  **service:** raise exception on uninitialized projects
   (`#1624 <https://github.com/SwissDataScienceCenter/renku-python/issues/1624>`__)
   (`a2025c3 <https://github.com/SwissDataScienceCenter/renku-python/commit/a2025c317c75371aec692d79882c3089c211bfaa>`__)

Features
~~~~~~~~

-  **cli:** add click plugin support
   (`#1604 <https://github.com/SwissDataScienceCenter/renku-python/issues/1604>`__)
   (`47b007f <https://github.com/SwissDataScienceCenter/renku-python/commit/47b007ff782432d75b0a9fd71476581ddc2d62dc>`__)
-  **cli:** adds consistent behaviour for cli commands
   (`#1523 <https://github.com/SwissDataScienceCenter/renku-python/issues/1523>`__)
   (`20b7248 <https://github.com/SwissDataScienceCenter/renku-python/commit/20b7248be39093a616c237e7c71700eda1271bc3>`__)
-  **cli:** show lfs status of dataset files
   (`#1575 <https://github.com/SwissDataScienceCenter/renku-python/issues/1575>`__)
   (`a1c3e2a <https://github.com/SwissDataScienceCenter/renku-python/commit/a1c3e2af84d502f1e67288ea791ef2f910bc67af>`__)
-  **cli:** verbose output for renku show
   (`#1524 <https://github.com/SwissDataScienceCenter/renku-python/issues/1524>`__)
   (`dae968c <https://github.com/SwissDataScienceCenter/renku-python/commit/dae968cdf0052c47603f0b8189ea24ee0b410d9a>`__)
-  **core:** Adds renku dataset update for Zenodo and Dataverse
   (`#1331 <https://github.com/SwissDataScienceCenter/renku-python/issues/1331>`__)
   (`e38c51f <https://github.com/SwissDataScienceCenter/renku-python/commit/e38c51f084c2d7e59088de385f7eae035043f9c1>`__)
-  **dataset:** list dataset description
   (`#1588 <https://github.com/SwissDataScienceCenter/renku-python/issues/1588>`__)
   (`7e13857 <https://github.com/SwissDataScienceCenter/renku-python/commit/7e1385796336741afef2044183bf853cdaab31d3>`__)
-  **service:** adds template and dockerfile migration to migration
   endpoint
   (`#1509 <https://github.com/SwissDataScienceCenter/renku-python/issues/1509>`__)
   (`ea01795 <https://github.com/SwissDataScienceCenter/renku-python/commit/ea017959de835e2a6882b932c480113f87d33502>`__)
-  **service:** adds version endpoint
   (`#1548 <https://github.com/SwissDataScienceCenter/renku-python/issues/1548>`__)
   (`6193df6 <https://github.com/SwissDataScienceCenter/renku-python/commit/6193df622cc9e9635f41bbb645a0adee3299043b>`__)

`0.11.6 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.5...v0.11.6>`__ (2020-10-16)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** fix bug where remote_cache caused project ids to leak
   (`#1618 <https://github.com/SwissDataScienceCenter/renku-python/pull/1618>`__)
   (`3ef04fb <https://github.com/SwissDataScienceCenter/renku-python/commit/3ef04fb8b6091d87b235901f7dfa22728f67b540>`__)
-  **dataset:** fix a bug where datasets imported from renku project won't update
   (`#1615 <https://github.com/SwissDataScienceCenter/renku-python/issues/1615>`__)
   (`309eb2f <https://github.com/SwissDataScienceCenter/renku-python/commit/309eb2fa8be53fa085222a98625fa0daffeee08f>`__)
-  **service:** fixes pushing to protected branches
   (`#1614 <https://github.com/SwissDataScienceCenter/renku-python/pull/1614>`__)
   (`34c7f92 <https://github.com/SwissDataScienceCenter/renku-python/commit/34c7f921ea04bfbfdbf08c85da165414efc315a6>`__)

`0.11.5 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.4...v0.11.5>`__ (2020-10-13)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** fix importing a dataset referenced from non-existent projects
   (`#1574 <https://github.com/SwissDataScienceCenter/renku-python/issues/1574>`__)
   (`4bb13ef <https://github.com/SwissDataScienceCenter/renku-python/commit/4bb13ef28a2bda2b3227deecb296e1274878752a>`__)
-  **core:** fixes git encoding of paths with unicode characters
   (`#1538 <https://github.com/SwissDataScienceCenter/renku-python/issues/1538>`__)
   (`9790707 <https://github.com/SwissDataScienceCenter/renku-python/commit/979070798968b2c2c624be5eceb4a0162c0faf80>`__)
-  **dataset:** fix broken directory hierarchy after renku dataset imports
   (`#1576 <https://github.com/SwissDataScienceCenter/renku-python/issues/1576>`__)
   (`41e3e72 <https://github.com/SwissDataScienceCenter/renku-python/commit/41e3e72024fe9a1a51521f739f920a63eceb95a8>`__)
-  **dataset:** abort importing a dataset when cannot pull LFS objects
   (`#1590 <https://github.com/SwissDataScienceCenter/renku-python/issues/1590>`__)
   (`9877a98 <https://github.com/SwissDataScienceCenter/renku-python/commit/9877a98c736f3a599ac9d7f0d59ac74af96d3da8>`__)
-  **dataset:** fix invalid dataset name after migration
   (`#1593 <https://github.com/SwissDataScienceCenter/renku-python/issues/1593>`__)
   (`c7ec249 <https://github.com/SwissDataScienceCenter/renku-python/commit/c7ec24936f2dac316cf737eef08ecf5f79d35973>`__)
-  **dataset:** update dataset files metadata when adding and overwriting local files
   (`#1582 <https://github.com/SwissDataScienceCenter/renku-python/issues/1582>`__)
   (`0a23e82 <https://github.com/SwissDataScienceCenter/renku-python/commit/0a23e82a19945b048a986fb679b84c88107ec124>`__)

`0.11.4 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.3...v0.11.4>`__ (2020-10-05)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** fix project migration getting overwritten with old metadata
   (`#1580 <https://github.com/SwissDataScienceCenter/renku-python/issues/1580>`__)
   (`dcc1541 <https://github.com/SwissDataScienceCenter/renku-python/commit/dcc1541f6164e697a7bc26054f7f6812bd1ef33a>`__)

`0.11.3 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.2...v0.11.3>`__ (2020-09-29)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** make Run migration ids unique by relative path instead of absolute
   (`686b9f9 <https://github.com/SwissDataScienceCenter/renku-python/commit/686b9f99d37315657bf9bcf273a4b0cf011d51ff>`__)

`0.11.2 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.1...v0.11.2>`__ (2020-09-24)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **cli:** fixes libxslt dependency in docker image
   (`#1534 <https://github.com/SwissDataScienceCenter/renku-python/issues/1534>`__)
   (`491bae7 <https://github.com/SwissDataScienceCenter/renku-python/commit/491bae764db72f91f3c444bd7105017e50e370cd>`__)
-  **core:** fixes ‘doi:…’ import
   (`#1536 <https://github.com/SwissDataScienceCenter/renku-python/issues/1536>`__)
   (`f653c79 <https://github.com/SwissDataScienceCenter/renku-python/commit/f653c7912ee03ca24e2043063a5b269d938b3c31>`__)
-  **core:** fixes duplicate ‘renku:Run’ ids on repeat execution of migrations
   (`#1532 <https://github.com/SwissDataScienceCenter/renku-python/issues/1532>`__)
   (`4ce6f3c <https://github.com/SwissDataScienceCenter/renku-python/commit/4ce6f3c1a86d9172973f78bea42619223c4fad2e>`__)

Features
~~~~~~~~

-  **cli:** show existing paths when initializing non-empty dir
   (`#1535 <https://github.com/SwissDataScienceCenter/renku-python/issues/1535>`__)
   (`07c559f <https://github.com/SwissDataScienceCenter/renku-python/commit/07c559fd1d8ff22b0aec25a9585019d847635dd8>`__)
-  **core:** follow URL redirections for dataset files
   (`#1516 <https://github.com/SwissDataScienceCenter/renku-python/issues/1516>`__)
   (`5a37b3c <https://github.com/SwissDataScienceCenter/renku-python/commit/5a37b3cdae7c06aca5b103bca8046e2804269765>`__)
-  **dataset:** flattened JSON-LD metadata
   (`#1518 <https://github.com/SwissDataScienceCenter/renku-python/issues/1518>`__)
   (`458ddb9 <https://github.com/SwissDataScienceCenter/renku-python/commit/458ddb9b9cea037c01be496c20573a7645b5c0e4>`__)
-  **service:** add additional template parameters
   (`#1469 <https://github.com/SwissDataScienceCenter/renku-python/issues/1469>`__)
   (`6372a32 <https://github.com/SwissDataScienceCenter/renku-python/commit/6372a32cd24dde4a77d53731ba03b6644b47ba73>`__)
-  **service:** adds additional fields to datasets listings
   (`#1508 <https://github.com/SwissDataScienceCenter/renku-python/issues/1508>`__)
   (`f8a395f <https://github.com/SwissDataScienceCenter/renku-python/commit/f8a395f90c2cf4be54c9da390638fcdb9cb0b8cc>`__)
-  **service:** adds project details and renku operation on jobs
   endpoint
   (`#1492 <https://github.com/SwissDataScienceCenter/renku-python/issues/1492>`__)
   (`6b3fafd <https://github.com/SwissDataScienceCenter/renku-python/commit/6b3fafd75a3eda7a481a5ff2f626c60a083413ef>`__)
-  **service:** execute read operations via git remote
   (`#1488 <https://github.com/SwissDataScienceCenter/renku-python/issues/1488>`__)
   (`84a0eb3 <https://github.com/SwissDataScienceCenter/renku-python/commit/84a0eb38a6521f12c8db05fe4ee169551fcc08a0>`__)
-  **workflow:** avoid unnecessary parent runs
   (`#1476 <https://github.com/SwissDataScienceCenter/renku-python/issues/1476>`__)
   (`b908ffd <https://github.com/SwissDataScienceCenter/renku-python/commit/b908ffd03a26e996f2df2127dbfdb66cd9da1dba>`__)

`0.11.1 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.11.0...v0.11.1>`__ (2020-08-18)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  fixes shacl for DatasetFile when used inside a qualifiedGeneration
   (`#1477 <https://github.com/SwissDataScienceCenter/renku-python/issues/1477>`__)
   (`99dd4a4 <https://github.com/SwissDataScienceCenter/renku-python/commit/99dd4a49704a36e330ea0d5ee7e0fcc7db31f7ad>`__)

`0.11.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.10.4...v0.11.0.>`__ (2020-08-14)
-----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **cli:** disable version check in githook calls
   (`#1300 <https://github.com/SwissDataScienceCenter/renku-python/issues/1300>`__)
   (`5132db3 <https://github.com/SwissDataScienceCenter/renku-python/commit/5132db3813dd0bd87f289dbf286ea73149478aeb>`__)
-  **core:** fix paths in migration of workflows
   (`#1371 <https://github.com/SwissDataScienceCenter/renku-python/issues/1371>`__)
   (`8c3d34b <https://github.com/SwissDataScienceCenter/renku-python/commit/8c3d34b526dbec5147d5ec4196669dcf3084930a>`__)
-  **core:** Fixes SoftwareAgent person context
   (`#1323 <https://github.com/SwissDataScienceCenter/renku-python/issues/1323>`__)
   (`a207a7f <https://github.com/SwissDataScienceCenter/renku-python/commit/a207a7f999d68373133801ac4ee3e08c06959ff3>`__)
-  **core:** Only update project metadata if any migrations were
   executed
   (`#1308 <https://github.com/SwissDataScienceCenter/renku-python/issues/1308>`__)
   (`1056a03 <https://github.com/SwissDataScienceCenter/renku-python/commit/1056a0363c85fe788b1a9a152c3674e4b093e827>`__)
-  **service:** adds more custom logging and imp. except handling
   (`#1435 <https://github.com/SwissDataScienceCenter/renku-python/issues/1435>`__)
   (`6c3adb5 <https://github.com/SwissDataScienceCenter/renku-python/commit/6c3adb510ca102f07e143b8c74aafaf29850656c>`__)
-  **service:** fixes handlers for internal loggers
   (`#1433 <https://github.com/SwissDataScienceCenter/renku-python/issues/1433>`__)
   (`a312f7c <https://github.com/SwissDataScienceCenter/renku-python/commit/a312f7c641c27de111fb78012c2f546405eee72e>`__)
-  **service:** move project_id to query string on migrations check
   (`#1367 <https://github.com/SwissDataScienceCenter/renku-python/issues/1367>`__)
   (`0f89726 <https://github.com/SwissDataScienceCenter/renku-python/commit/0f8972698399aee50bced4a141cb24add6d9494b>`__)
-  **tests:** integration tests
   (`#1351 <https://github.com/SwissDataScienceCenter/renku-python/issues/1351>`__)
   (`3974a39 <https://github.com/SwissDataScienceCenter/renku-python/commit/3974a39d5098ecebcc49f2d624e6912e1d423e03>`__)

Features
~~~~~~~~

-  **cli:** Adds renku save command
   (`#1273 <https://github.com/SwissDataScienceCenter/renku-python/issues/1273>`__)
   (`4ddc1c2 <https://github.com/SwissDataScienceCenter/renku-python/commit/4ddc1c2cf8db05fecc3634bc33f9d5df8488d017>`__)
-  **cli:** prompt for missing variables
   (`1e1d408 <https://github.com/SwissDataScienceCenter/renku-python/commit/1e1d4087e9a4fc338b64e050c4b345f298d9eb3f>`__),
   closes
   `#1126 <https://github.com/SwissDataScienceCenter/renku-python/issues/1126>`__
-  **cli:** Show detailed commands for renku log output
   (`#1345 <https://github.com/SwissDataScienceCenter/renku-python/issues/1345>`__)
   (`19fb819 <https://github.com/SwissDataScienceCenter/renku-python/commit/19fb8194003646c4ae78009a3714ac8ff89f4edb>`__)
-  **core:** Calamus integration
   (`#1281 <https://github.com/SwissDataScienceCenter/renku-python/issues/1281>`__)
   (`bda538f <https://github.com/SwissDataScienceCenter/renku-python/commit/bda538fb3733d222003b62bf7821ce02f44afac7>`__)
-  **core:** configurable data dir
   (`#1347 <https://github.com/SwissDataScienceCenter/renku-python/issues/1347>`__)
   (`e388773 <https://github.com/SwissDataScienceCenter/renku-python/commit/e388773b0cc70219a8791ed0a34c07cb8fb3a00b>`__)
-  **core:** disabling of inputs/outputs auto-detection
   (`#1406 <https://github.com/SwissDataScienceCenter/renku-python/issues/1406>`__)
   (`3245ca0 <https://github.com/SwissDataScienceCenter/renku-python/commit/3245ca077a9937241836617ff73756bd1602ee60>`__)
-  **core:** migration check in core
   (`#1320 <https://github.com/SwissDataScienceCenter/renku-python/issues/1320>`__)
   (`4bc52f4 <https://github.com/SwissDataScienceCenter/renku-python/commit/4bc52f430b0def821df7cb3dc498af46ee9c4448>`__)
-  **core:** Move workflow serialisation over to calamus
   (`#1386 <https://github.com/SwissDataScienceCenter/renku-python/issues/1386>`__)
   (`f0fbc49 <https://github.com/SwissDataScienceCenter/renku-python/commit/f0fbc4979955de04ac5c565e471a52019eb6cd48>`__)
-  **core:** save and load workflow as jsonld
   (`#1185 <https://github.com/SwissDataScienceCenter/renku-python/issues/1185>`__)
   (`d403289 <https://github.com/SwissDataScienceCenter/renku-python/commit/d403289cec6cdb11ad002b17fbbb751de2818ce1>`__)
-  **core:** separate models for migrations
   (`#1431 <https://github.com/SwissDataScienceCenter/renku-python/issues/1431>`__)
   (`127d606 <https://github.com/SwissDataScienceCenter/renku-python/commit/127d606fa9ad26479645a06b132a7c9a952ae989>`__)
-  **dataset:** source and url for DatasetFile
   (`#1451 <https://github.com/SwissDataScienceCenter/renku-python/issues/1451>`__)
   (`b4fa5db <https://github.com/SwissDataScienceCenter/renku-python/commit/b4fa5dbed50b47a6a64f3c84b40bb99d86abd51e>`__)
-  **service:** added endpoints to execute all migrations on a project
   (`#1322 <https://github.com/SwissDataScienceCenter/renku-python/issues/1322>`__)
   (`aca8cc2 <https://github.com/SwissDataScienceCenter/renku-python/commit/aca8cc2f540041d5e5d5c06e180d62cd52876086>`__)
-  **service:** adds endpoint for explicit migrations check
   (`#1326 <https://github.com/SwissDataScienceCenter/renku-python/issues/1326>`__)
   (`146b1a7 <https://github.com/SwissDataScienceCenter/renku-python/commit/146b1a7a579898110fdf95ffc67ebc813115cc2e>`__)
-  **service:** adds source and destination versions to migrations check
   (`#1372 <https://github.com/SwissDataScienceCenter/renku-python/issues/1372>`__)
   (`ea76b48 <https://github.com/SwissDataScienceCenter/renku-python/commit/ea76b48a90e5f476f057a939d661f39d09f1b4c3>`__)
-  decode base64 headers
   (`#1407 <https://github.com/SwissDataScienceCenter/renku-python/issues/1407>`__)
   (`9901cc3 <https://github.com/SwissDataScienceCenter/renku-python/commit/9901cc339f9832a5161b82d4368f239d3929f740>`__)
-  **service:** adds endpoints for dataset remove
   (`#1383 <https://github.com/SwissDataScienceCenter/renku-python/issues/1383>`__)
   (`289e4b9 <https://github.com/SwissDataScienceCenter/renku-python/commit/289e4b9fec4cec18e9fb82e2360b00f562b7b09b>`__)
-  **service:** adds endpoints for unlinking files from a dataset
   (`#1314 <https://github.com/SwissDataScienceCenter/renku-python/issues/1314>`__)
   (`1b78b16 <https://github.com/SwissDataScienceCenter/renku-python/commit/1b78b16d97ba42d08505fb6b38fa2d66c6209417>`__)
-  **service:** async migrations execution
   (`#1344 <https://github.com/SwissDataScienceCenter/renku-python/issues/1344>`__)
   (`ff66953 <https://github.com/SwissDataScienceCenter/renku-python/commit/ff66953de06d8abdd6ca8067aaa7d48ae24423de>`__)
-  **service:** create new projects from templates
   (`#1287 <https://github.com/SwissDataScienceCenter/renku-python/issues/1287>`__)
   (`552f85c <https://github.com/SwissDataScienceCenter/renku-python/commit/552f85ce7392a727db6b2c2bcef35ecbbfa93913>`__),
   closes
   `#862 <https://github.com/SwissDataScienceCenter/renku-python/issues/862>`__

`0.10.5 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.10.4...v0.10.5>`__ (2020-07-16)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** Pin dependencies to prevent downstream dependency updates from breaking renku. Fix pyshacl dependency.
   (`#785 <https://github.com/SwissDataScienceCenter/renku-python/issues/785>`__)
   (`30beedd <https://github.com/SwissDataScienceCenter/renku-python/pull/1391/commits/30beedd44f55e028eec4dbc6c786fd1eb9747d08>`__)

- **core:** Fixes SoftwareAgent person context.
  (`#1323 <https://github.com/SwissDataScienceCenter/renku-python/pull/1323>`__)
  (`fa62f58 <https://github.com/SwissDataScienceCenter/renku-python/pull/1391/commits/fa62f587d29f413998c1aa04a5cb7585c32700a6>`__)


`0.10.4 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.10.3...v0.10.4>`__ (2020-05-18)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **dataset:** update default behaviour and messaging on dataset unlink
   (`#1275 <https://github.com/SwissDataScienceCenter/renku-python/issues/1275>`__)
   (`98d6728 <https://github.com/SwissDataScienceCenter/renku-python/commit/98d67284668f3f1c0d37aba52dc6a5557e8fc806>`__)
-  **dataset:** correct url in different domain
   (`#1211 <https://github.com/SwissDataScienceCenter/renku-python/issues/1211>`__)
   (`49e8b8b <https://github.com/SwissDataScienceCenter/renku-python/commit/49e8b8bf21c1bacae3eb580f8e3043b79e823ff5>`__)

Features
~~~~~~~~

-  **cli:** Adds warning messages for LFS, fix output redirection
   (`#1199 <https://github.com/SwissDataScienceCenter/renku-python/issues/1199>`__)
   (`31969f5 <https://github.com/SwissDataScienceCenter/renku-python/commit/31969f54f36b5fbbf827f81411929b991580e1fa>`__)
-  **core:** Adds lfs file size limit and lfs ignore file
   (`#1210 <https://github.com/SwissDataScienceCenter/renku-python/issues/1210>`__)
   (`1f3c81c <https://github.com/SwissDataScienceCenter/renku-python/commit/1f3c81c9a4faed3e520a01e8881c08f437d871cf>`__)
-  **core:** Adds renku storage clean command
   (`#1235 <https://github.com/SwissDataScienceCenter/renku-python/issues/1235>`__)
   (`7029400 <https://github.com/SwissDataScienceCenter/renku-python/commit/7029400ef2750c0ff6d5ab55387c21353baee4c2>`__)
-  **core:** git hook to avoid committing large files
   (`#1238 <https://github.com/SwissDataScienceCenter/renku-python/issues/1238>`__)
   (`e8f1a8b <https://github.com/SwissDataScienceCenter/renku-python/commit/e8f1a8b811222f80dbc765e177d73ca8db7db290>`__)
-  **core:** renku doctor check for lfs migrate info
   (`#1234 <https://github.com/SwissDataScienceCenter/renku-python/issues/1234>`__)
   (`480da06 <https://github.com/SwissDataScienceCenter/renku-python/commit/480da06e453b3a5770467accec070a5d84ed5951>`__)
-  **dataset:** fail early when external storage not installed
   (`#1239 <https://github.com/SwissDataScienceCenter/renku-python/issues/1239>`__)
   (`e6ea6da <https://github.com/SwissDataScienceCenter/renku-python/commit/e6ea6dade55f928681795395c316cd9de9116845>`__)
-  **core:** project clone API support for revision checkout
   (`#1208 <https://github.com/SwissDataScienceCenter/renku-python/issues/1208>`__)
   (`74116e9 <https://github.com/SwissDataScienceCenter/renku-python/commit/74116e9ae0c37dbb1e5f772695288aaefaac66ec>`__)
-  **service:** protected branches support
   (`#1222 <https://github.com/SwissDataScienceCenter/renku-python/issues/1222>`__)
   (`8405ce5 <https://github.com/SwissDataScienceCenter/renku-python/commit/8405ce5ad1d3aa3f10c583aff2e7ca283db5033e>`__)
-  **dataset:** doi variations for import
   (`#1216 <https://github.com/SwissDataScienceCenter/renku-python/issues/1216>`__)
   (`0f329dd <https://github.com/SwissDataScienceCenter/renku-python/commit/0f329dd1796d61838bef89f66ff1bb88af243059>`__)
-  **dataset:** keywords in metadata
   (`#1209 <https://github.com/SwissDataScienceCenter/renku-python/issues/1209>`__)
   (`f98a800 <https://github.com/SwissDataScienceCenter/renku-python/commit/f98a800c07d901d7461be9250fe6957325b2adcb>`__)
-  **dataset:** no failure when adding ignored files
   (`#1213 <https://github.com/SwissDataScienceCenter/renku-python/issues/1213>`__)
   (`b1e275f <https://github.com/SwissDataScienceCenter/renku-python/commit/b1e275fc59999fe6b146e556b073a65dc1726bb5>`__)
-  **service:** read template manifest
   (`#1254 <https://github.com/SwissDataScienceCenter/renku-python/issues/1254>`__)
   (`7eac85b <https://github.com/SwissDataScienceCenter/renku-python/commit/7eac85b766c9deeaba44e1adbc89900b952bef0d>`__)


`0.10.3 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.10.2...v0.10.3>`__ (2020-04-22)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **dataset:** avoid recursive addition of data directory
   (`#1163 <https://github.com/SwissDataScienceCenter/renku-python/issues/1163>`__)
   (`79e3b03 <https://github.com/SwissDataScienceCenter/renku-python/commit/79e3b0346df89dbb445df769dfa9dca0f878a242>`__)
-  **dataset:** commit after unlinking files
   (`#1120 <https://github.com/SwissDataScienceCenter/renku-python/issues/1120>`__)
   (`97e8754 <https://github.com/SwissDataScienceCenter/renku-python/commit/97e8754729078a54f20876ddaaf9ca191f4cb59a>`__)
-  **dataset:** Dataverse export
   (`#1028 <https://github.com/SwissDataScienceCenter/renku-python/issues/1028>`__)
   (`737cecf <https://github.com/SwissDataScienceCenter/renku-python/commit/737cecfebb298daa5704bda0ead73ebff1312539>`__)

Features
~~~~~~~~

-  **core:** CLI warning when in non-root directory
   (`#1162 <https://github.com/SwissDataScienceCenter/renku-python/issues/1162>`__)
   (`115e462 <https://github.com/SwissDataScienceCenter/renku-python/commit/115e46227decd7a4597a0918f3bae279fc73e366>`__)
-  **dataset:** migrate submodule-based datasets
   (`#1092 <https://github.com/SwissDataScienceCenter/renku-python/issues/1092>`__)
   (`dba20c4 <https://github.com/SwissDataScienceCenter/renku-python/commit/dba20c467ecf60361785bf515cd80633765db518>`__)
-  **dataset:** no failure when adding existing files
   (`#1177 <https://github.com/SwissDataScienceCenter/renku-python/issues/1177>`__)
   (`a68dcb7 <https://github.com/SwissDataScienceCenter/renku-python/commit/a68dcb789d9e9103a64fd678686256566a92d25c>`__)
-  **dataset:** remove –link flag
   (`#1164 <https://github.com/SwissDataScienceCenter/renku-python/issues/1164>`__)
   (`969d4f8 <https://github.com/SwissDataScienceCenter/renku-python/commit/969d4f88a082437e7f55a123347c2f63f32d5593>`__)
-  **dataset:** show file size in ls-files
   (`#1123 <https://github.com/SwissDataScienceCenter/renku-python/issues/1123>`__)
   (`0951930 <https://github.com/SwissDataScienceCenter/renku-python/commit/095193038d41c60e503d0dd52d8de2fca7ce29f8>`__)
-  **datasets:** specify title on dataset creation
   (`#1204 <https://github.com/SwissDataScienceCenter/renku-python/issues/1204>`__)
   (`fb70ac5 <https://github.com/SwissDataScienceCenter/renku-python/commit/fb70ac50533bfdb1b1cfb8d1e163c40531ea35fe>`__)
-  **init:** read and display template variables
   (`#1134 <https://github.com/SwissDataScienceCenter/renku-python/issues/1134>`__)
   (`0f86dc5 <https://github.com/SwissDataScienceCenter/renku-python/commit/0f86dc5fde89274a59f11345a5cc106fcbd8b206>`__),
   closes
   `#1126 <https://github.com/SwissDataScienceCenter/renku-python/issues/1126>`__
-  **service:** add remote files to dataset
   (`#1139 <https://github.com/SwissDataScienceCenter/renku-python/issues/1139>`__)
   (`f6bebfe <https://github.com/SwissDataScienceCenter/renku-python/commit/f6bebfea203c63bcf0843b48377b786a97aaa535>`__)


`0.10.1 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.10.0...v0.10.1>`__ (2020-03-31)
----------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  **core:** directory input regression
   (`#1155 <https://github.com/SwissDataScienceCenter/renku-python/issues/1155>`__)
   (`b17c843 <https://github.com/SwissDataScienceCenter/renku-python/commit/b17c84382c68eee78f6fbaddc2b18794e72f30a3>`__)
-  **dataset:** correct url when importing with DOI
   (`#1156 <https://github.com/SwissDataScienceCenter/renku-python/issues/1156>`__)
   (`025b735 <https://github.com/SwissDataScienceCenter/renku-python/commit/025b735dfbe34e2bd58e85c2cbd6722b328fccef>`__)

Features
~~~~~~~~

-  renku init options refactor
   (`#1127 <https://github.com/SwissDataScienceCenter/renku-python/issues/1127>`__)
   (`78b208b <https://github.com/SwissDataScienceCenter/renku-python/commit/78b208bf9800a0c4caf85b4eed20bf5656561e37>`__)
-  **datasets:** add files from dropbox
   (`#1135 <https://github.com/SwissDataScienceCenter/renku-python/issues/1135>`__)
   (`bf5f2db <https://github.com/SwissDataScienceCenter/renku-python/commit/bf5f2db34479ed574af9fd57a893884abddb6f41>`__)


`0.10.0 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.9.1...v0.10.0>`__ (2020-03-25)
---------------------------------------------------------------------------------------------------------

This release brings about several important Dataset features:

- importing renku datasets (`#838 <https://github.com/SwissDataScienceCenter/renku-python/issues/838>`__)
- working with data external to the repository (`#974 <https://github.com/SwissDataScienceCenter/renku-python/issues/974>`__)
- editing dataset metadata (`#1111 <https://github.com/SwissDataScienceCenter/renku-python/issues/1111>`__)

Please see the `Dataset documentation
<https://renku-python.readthedocs.io/en/latest/commands.html#module-renku.cli.dataset>`_ for details.

Additional features were implemented for the backend service to facilitate a smoother user
experience for dataset file manipulation.

**IMPORTANT**: starting with this version, a new metadata migration mechanism is in place
(`#1003 <https://github.com/SwissDataScienceCenter/renku-python/issues/1003>`__). Renku commands
will insist on migrating a project immediately if the metadata is found to be outdated.


Bug Fixes
~~~~~~~~~

-  **cli:** consistenly show correct contexts
   (`#1096 <https://github.com/SwissDataScienceCenter/renku-python/issues/1096>`__)
   (`b333f0f <https://github.com/SwissDataScienceCenter/renku-python/commit/b333f0f27fdaeec1ce3ba7a145739055429419d9>`__)
-  **dataset:** –no-external-storage flag not working
   (`#1130 <https://github.com/SwissDataScienceCenter/renku-python/issues/1130>`__)
   (`c183e97 <https://github.com/SwissDataScienceCenter/renku-python/commit/c183e97940914cca1b7415722b4af2f47ad082c0>`__)
-  **dataset:** commit only updated dataset files
   (`#1116 <https://github.com/SwissDataScienceCenter/renku-python/issues/1116>`__)
   (`d9739df <https://github.com/SwissDataScienceCenter/renku-python/commit/d9739dfee081181a4801373a85a057267945da1f>`__)
-  **datasets:** fixed importing large amount of small files
   (`#1119 <https://github.com/SwissDataScienceCenter/renku-python/issues/1119>`__)
   (`8d61473 <https://github.com/SwissDataScienceCenter/renku-python/commit/8d61473bb6630d4b75c0d95a3d001ea2a5bcfbb7>`__)
-  **datasets:** raises correct error message on import of protected
   dataset
   (`#1112 <https://github.com/SwissDataScienceCenter/renku-python/issues/1112>`__)
   (`e579904 <https://github.com/SwissDataScienceCenter/renku-python/commit/e579904acd4aa944476b4990e881ad6416efffb4>`__)

Features
~~~~~~~~

-  **core:** new migration mechanism
   (`#1003 <https://github.com/SwissDataScienceCenter/renku-python/issues/1003>`__)
   (`1cc33d4 <https://github.com/SwissDataScienceCenter/renku-python/commit/1cc33d46918468ec82fa5829d4e7bd03e7e25b77>`__)
-  **dataset:** adding external data without copying
   (`#974 <https://github.com/SwissDataScienceCenter/renku-python/issues/974>`__)
   (`6a17512 <https://github.com/SwissDataScienceCenter/renku-python/commit/6a17512e416737419b370c730172117b338dd528>`__)

-  **dataset:** bypass import confirmation
   (`#1124 <https://github.com/SwissDataScienceCenter/renku-python/issues/1124>`__)
   (`947210a <https://github.com/SwissDataScienceCenter/renku-python/commit/947210ae4432ef6eb6d3a902d239871d405abbbf>`__)
-  **dataset:** import renku datasets
   (`#838 <https://github.com/SwissDataScienceCenter/renku-python/issues/838>`__)
   (`6aa3651 <https://github.com/SwissDataScienceCenter/renku-python/commit/6aa3651398432a938d29a2c355541170d496e2df>`__)
-  **dataset:** metadata edit
   (`#1111 <https://github.com/SwissDataScienceCenter/renku-python/issues/1111>`__)
   (`66cfbbc <https://github.com/SwissDataScienceCenter/renku-python/commit/66cfbbc81c274822ed339b1919e6ac10646ab5fe>`__)
-  **dataset:** wildcard support when adding data from git
   (`#1128 <https://github.com/SwissDataScienceCenter/renku-python/issues/1128>`__)
   (`baa1c9f <https://github.com/SwissDataScienceCenter/renku-python/commit/baa1c9f840eec9c2c0ae12723c28db1728c104eb>`__)


`0.9.1 <https://github.com/SwissDataScienceCenter/renku-python/compare/v0.9.0...v0.9.1>`__ (2020-02-24)
-------------------------------------------------------------------------------------------------------

Bug Fixes
~~~~~~~~~

-  added test utility functions and cleanup
   (`#1014 <https://github.com/SwissDataScienceCenter/renku-python/issues/1014>`__)
   (`f41100d <https://github.com/SwissDataScienceCenter/renku-python/commit/f41100de7c6a30e1cf6d0cd49faedba82200c5df>`__)
-  cache instance cleanup
   (`#1051 <https://github.com/SwissDataScienceCenter/renku-python/issues/1051>`__)
   (`12f5446 <https://github.com/SwissDataScienceCenter/renku-python/commit/12f5446ca00ca9bbb9466f4d41f52a0e4d06466d>`__)
-  enable dataset cmd in sub directories
   (`#1012 <https://github.com/SwissDataScienceCenter/renku-python/issues/1012>`__)
   (`e3191e1 <https://github.com/SwissDataScienceCenter/renku-python/commit/e3191e154b7e5c830ab915884c2dae17e9450cc6>`__)
-  fields with default need to come last
   (`#1046 <https://github.com/SwissDataScienceCenter/renku-python/issues/1046>`__)
   (`649b159 <https://github.com/SwissDataScienceCenter/renku-python/commit/649b15928a6c8791989e953933f676a75b7a29c7>`__)
-  fixes renku show sibling handling with no paths
   (`#1026 <https://github.com/SwissDataScienceCenter/renku-python/issues/1026>`__)
   (`8df678f <https://github.com/SwissDataScienceCenter/renku-python/commit/8df678f887d33ea75ab118c3204fc105bcd188ed>`__)
-  flush old keys for user projects and files
   (`#1002 <https://github.com/SwissDataScienceCenter/renku-python/issues/1002>`__)
   (`7438c73 <https://github.com/SwissDataScienceCenter/renku-python/commit/7438c7353433d60269d4adef0bd2fc49db02c029>`__)
-  generate https IDs for entities instead of file://
   (`#1009 <https://github.com/SwissDataScienceCenter/renku-python/issues/1009>`__)
   (`87f7750 <https://github.com/SwissDataScienceCenter/renku-python/commit/87f7750e3841d6b2784656bfb6f56106ccdbf305>`__)
-  handle errors correctly
   (`#1040 <https://github.com/SwissDataScienceCenter/renku-python/issues/1040>`__)
   (`950eeac <https://github.com/SwissDataScienceCenter/renku-python/commit/950eeac1eea619fa07bccd27e4fad54c42691e58>`__)
-  improved list datasets and files
   (`#1034 <https://github.com/SwissDataScienceCenter/renku-python/issues/1034>`__)
   (`fd96d68 <https://github.com/SwissDataScienceCenter/renku-python/commit/fd96d685fa44777d98c4e7d6d59368c93e130a29>`__)
-  pin idna to 2.8
   (`#1020 <https://github.com/SwissDataScienceCenter/renku-python/issues/1020>`__)
   (`19ea7af <https://github.com/SwissDataScienceCenter/renku-python/commit/19ea7af66414f8df605d328d7f10fadac828ccf5>`__)
-  resync repo after import action
   (`#1052 <https://github.com/SwissDataScienceCenter/renku-python/issues/1052>`__)
   (`b38341b <https://github.com/SwissDataScienceCenter/renku-python/commit/b38341be4b13df493daf285be8ef2cc985fabc8b>`__)
-  standardize test assertions
   (`#1016 <https://github.com/SwissDataScienceCenter/renku-python/issues/1016>`__)
   (`16e8e63 <https://github.com/SwissDataScienceCenter/renku-python/commit/16e8e636e2e0dd79c35e2f88c20a18c9022022f5>`__)
-  temporarily disable integration tests
   (`#1036 <https://github.com/SwissDataScienceCenter/renku-python/issues/1036>`__)
   (`8c8fd7a <https://github.com/SwissDataScienceCenter/renku-python/commit/8c8fd7afc228b188f739e3963536f4b58bfbb22d>`__)
-  updated readme to include local testing
   (`#1000 <https://github.com/SwissDataScienceCenter/renku-python/issues/1000>`__)
   (`351a650 <https://github.com/SwissDataScienceCenter/renku-python/commit/351a6500984ca248c8a2af7470eea343dcc7e4bf>`__)
-  run tests via pipenv run commands
   (`#999 <https://github.com/SwissDataScienceCenter/renku-python/issues/999>`__)
   (`d8095e3 <https://github.com/SwissDataScienceCenter/renku-python/commit/d8095e3a9aebd859999acfae49f4433448480937>`__)

Features
~~~~~~~~

-  **svc:** adds job details endpoint
   (`#1050 <https://github.com/SwissDataScienceCenter/renku-python/issues/1050>`__)
   (`9c58a08 <https://github.com/SwissDataScienceCenter/renku-python/commit/9c58a086c74ff3113901891b1a7be0290157714a>`__)
-  **svc:** added list user jobs endpoint
   (`#1001 <https://github.com/SwissDataScienceCenter/renku-python/issues/1001>`__)
   (`f3c200c <https://github.com/SwissDataScienceCenter/renku-python/commit/f3c200cae55716e5644a705efed74b6a3e9fcf40>`__)
-  **svc:** dataset import via service
   (`#1023 <https://github.com/SwissDataScienceCenter/renku-python/issues/1023>`__)
   (`d6c670a <https://github.com/SwissDataScienceCenter/renku-python/commit/d6c670a01bef863611b114896207e15045e8ac9a>`__)

.. _section-1:


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
