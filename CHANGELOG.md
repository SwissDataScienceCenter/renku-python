# [0.6.1](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.6.0...v) (2019-10-10)


### Bug Fixes

* add .renku/tmp to default .gitignore ([#728](https://github.com/SwissDataScienceCenter/renku-python/issues/728)) ([6212148](https://github.com/SwissDataScienceCenter/renku-python/commit/62121485119ed02a28a5b2e2990395bee284e348))
* dataset import causes renku exception due to duplicate LocalClient ([#724](https://github.com/SwissDataScienceCenter/renku-python/issues/724)) ([89411b0](https://github.com/SwissDataScienceCenter/renku-python/commit/89411b010a2671790aafccaa42216e5ccdf5a660))
* delete new dataset ref if file add fails ([#729](https://github.com/SwissDataScienceCenter/renku-python/issues/729)) ([2dea711](https://github.com/SwissDataScienceCenter/renku-python/commit/2dea711939a11c8c8c358bfd76499ae8865958ee))
* fixes bug with deleted files not getting committed ([#741](https://github.com/SwissDataScienceCenter/renku-python/issues/741)) ([5de4b6f](https://github.com/SwissDataScienceCenter/renku-python/commit/5de4b6feaa17d4db3fbec8e65523c28cfd80ca3e))
* force current project for entities ([#707](https://github.com/SwissDataScienceCenter/renku-python/issues/707)) ([538ef07](https://github.com/SwissDataScienceCenter/renku-python/commit/538ef078b95c78d43907294147ca4fd359779b9e))
* integration tests for [#681](https://github.com/SwissDataScienceCenter/renku-python/issues/681) ([#747](https://github.com/SwissDataScienceCenter/renku-python/issues/747)) ([b08435d](https://github.com/SwissDataScienceCenter/renku-python/commit/b08435d26a19e69bff9a9b376526aee9ab3ecd70))
* use commit author for project creator ([#715](https://github.com/SwissDataScienceCenter/renku-python/issues/715)) ([1a40ebe](https://github.com/SwissDataScienceCenter/renku-python/commit/1a40ebe75ea7c49cf52a9fce5c757d7352f159b0)), closes [#713](https://github.com/SwissDataScienceCenter/renku-python/issues/713)
* zenodo dataset import error ([f1d623a](https://github.com/SwissDataScienceCenter/renku-python/commit/f1d623abf16287d62a3a0fecaf40e16d4ed5fc8b))


### Features

* adds basic QA checklist ([#698](https://github.com/SwissDataScienceCenter/renku-python/issues/698)) ([c97e9bd](https://github.com/SwissDataScienceCenter/renku-python/commit/c97e9bdc0c65fb6b06c7467624a988bf533f0b80))
* dataset tagging ([#693](https://github.com/SwissDataScienceCenter/renku-python/issues/693)) ([797161f](https://github.com/SwissDataScienceCenter/renku-python/commit/797161fe48746137ae4c2761dfcbc7819124f154))
* include creator in project metadata ([#687](https://github.com/SwissDataScienceCenter/renku-python/issues/687)) ([9c7753e](https://github.com/SwissDataScienceCenter/renku-python/commit/9c7753e56c17642e73f5bb33bd41987c104bbd7c))
* use URIs for dataset ids ([#742](https://github.com/SwissDataScienceCenter/renku-python/issues/742)) ([a8992b9](https://github.com/SwissDataScienceCenter/renku-python/commit/a8992b913c82d0d4ee73cf64d39f0358930c8782)), closes [#719](https://github.com/SwissDataScienceCenter/renku-python/issues/719)



# [0.6.0](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.2...v0.6.0) (2019-09-19)


### Bug Fixes

* adds _label and commit data to imported dataset files, single commit for imports ([#651](https://github.com/SwissDataScienceCenter/renku-python/issues/651)) ([75ce369](https://github.com/SwissDataScienceCenter/renku-python/commit/75ce369e8f4d1ea0a29cb04784c6455b4fb5636f))
* always add commit to dataset if possible ([#648](https://github.com/SwissDataScienceCenter/renku-python/issues/648)) ([7659bc8](https://github.com/SwissDataScienceCenter/renku-python/commit/7659bc84d6531bd2e8329aac188fb4509794a837)), closes [#646](https://github.com/SwissDataScienceCenter/renku-python/issues/646)
* cleanup needed for integration tests on py35 ([#653](https://github.com/SwissDataScienceCenter/renku-python/issues/653)) ([fdd7215](https://github.com/SwissDataScienceCenter/renku-python/commit/fdd721526961c33366fcc26eef2b08281e9f9318))
* fixed serialization of datetime to iso format ([#629](https://github.com/SwissDataScienceCenter/renku-python/issues/629)) ([693d59d](https://github.com/SwissDataScienceCenter/renku-python/commit/693d59d431da742d5d918cc25452f4f940f766a4))
* fixes broken integration test ([#649](https://github.com/SwissDataScienceCenter/renku-python/issues/649)) ([04eba66](https://github.com/SwissDataScienceCenter/renku-python/commit/04eba66b70bcbcc22e80ee7149d2032b5352931e))
* hide image, pull, runner, show, workon and deactivate commands ([#672](https://github.com/SwissDataScienceCenter/renku-python/issues/672)) ([a3e9998](https://github.com/SwissDataScienceCenter/renku-python/commit/a3e9998a5154a4520751055e531797dbba3dee66))
* integration tests fixed ([#685](https://github.com/SwissDataScienceCenter/renku-python/issues/685)) ([f0ea8f0](https://github.com/SwissDataScienceCenter/renku-python/commit/f0ea8f0a8261b9de797882658a4ff124ac996fe6))
* migration of old datasets ([#639](https://github.com/SwissDataScienceCenter/renku-python/issues/639)) ([4d4d7d2](https://github.com/SwissDataScienceCenter/renku-python/commit/4d4d7d289bd7e83fec9b9e57fda568824d54bf1c))
* migration timezones ([#683](https://github.com/SwissDataScienceCenter/renku-python/issues/683)) ([58c2de4](https://github.com/SwissDataScienceCenter/renku-python/commit/58c2de4cc08a297911babe28f85729b94ba676a3))
* removes unneccesary call to git lfs with no paths ([#658](https://github.com/SwissDataScienceCenter/renku-python/issues/658)) ([e32d48b](https://github.com/SwissDataScienceCenter/renku-python/commit/e32d48bc8b7d34e130a3b26a2e57dcf6f8c2396f))
* renku home directory overwrite in tests ([#657](https://github.com/SwissDataScienceCenter/renku-python/issues/657)) ([90e1c48](https://github.com/SwissDataScienceCenter/renku-python/commit/90e1c48f019e342fd39a3971f704074bdf0527db))
* security update ([#633](https://github.com/SwissDataScienceCenter/renku-python/issues/633)) ([52d8989](https://github.com/SwissDataScienceCenter/renku-python/commit/52d89898735423b1eef69454c8ad77992d8a20fd))
* upload metadata before actual files ([#652](https://github.com/SwissDataScienceCenter/renku-python/issues/652)) ([95ed468](https://github.com/SwissDataScienceCenter/renku-python/commit/95ed468103d41d900565ac62cdc30262bf4046b6))
* use latest_html for version check ([#647](https://github.com/SwissDataScienceCenter/renku-python/issues/647)) ([c6b0309](https://github.com/SwissDataScienceCenter/renku-python/commit/c6b0309a2b5970bbb5b8d7e9915ed5ef8a3c0e36)), closes [#641](https://github.com/SwissDataScienceCenter/renku-python/issues/641)
* user-related metadata ([#655](https://github.com/SwissDataScienceCenter/renku-python/issues/655)) ([44183e6](https://github.com/SwissDataScienceCenter/renku-python/commit/44183e6d0653950e820e94308941d61c930951d2))
* zenodo export failing with relative paths ([d40967c](https://github.com/SwissDataScienceCenter/renku-python/commit/d40967c93d3de51d0b9941c82d07e1c9e27c8263))


### Features

* dataverse import ([#626](https://github.com/SwissDataScienceCenter/renku-python/issues/626)) ([9f0f9a1](https://github.com/SwissDataScienceCenter/renku-python/commit/9f0f9a1e8109cc7af5994fbbcb8ed9201d6cc431))
* enable all datasets command to operate on dirty repository ([#607](https://github.com/SwissDataScienceCenter/renku-python/issues/607)) ([74e328b](https://github.com/SwissDataScienceCenter/renku-python/commit/74e328bb6f5fde4697d40fbc15f5a71f05911649))
* explicit input output specification ([#598](https://github.com/SwissDataScienceCenter/renku-python/issues/598)) ([ce8ba67](https://github.com/SwissDataScienceCenter/renku-python/commit/ce8ba673780801135cec1356da21fcce35d21288))
* export filename as schema:name ([#643](https://github.com/SwissDataScienceCenter/renku-python/issues/643)) ([aed54bf](https://github.com/SwissDataScienceCenter/renku-python/commit/aed54bf77a003752d8c6b3f0895ab38558c86aa6)), closes [#640](https://github.com/SwissDataScienceCenter/renku-python/issues/640)
* support for indirect inputs and outputs ([#650](https://github.com/SwissDataScienceCenter/renku-python/issues/650)) ([e960a98](https://github.com/SwissDataScienceCenter/renku-python/commit/e960a98a3c0eeb795d718e90819c766a57d8180b))



## [0.5.2](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.1...v0.5.2) (2019-07-26)


### Bug Fixes

* safe_path check always operates on str ([#603](https://github.com/SwissDataScienceCenter/renku-python/issues/603)) ([7c1c34e](https://github.com/SwissDataScienceCenter/renku-python/commit/7c1c34ea1047802a927d4df187cd78d6dc9b41f6))


### Features

* add SoftwareAgent to Activity ([#590](https://github.com/SwissDataScienceCenter/renku-python/issues/590)) ([a60c20c](https://github.com/SwissDataScienceCenter/renku-python/commit/a60c20cba3575cdf5f30e83b4a9a809daf15438a)), closes [#508](https://github.com/SwissDataScienceCenter/renku-python/issues/508)



## [0.5.1](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.5.0...v0.5.1) (2019-07-12)


### Bug Fixes

* ensure external storage is handled correctly ([#592](https://github.com/SwissDataScienceCenter/renku-python/issues/592)) ([7938ac4](https://github.com/SwissDataScienceCenter/renku-python/commit/7938ac4e4ab66504ffeece1e3a8b773c80847c14))
* only check local repo for lfs filter ([#575](https://github.com/SwissDataScienceCenter/renku-python/issues/575)) ([a64dc79](https://github.com/SwissDataScienceCenter/renku-python/commit/a64dc79f9ad1449c7089759f5266ba76762c512a))
* **cli:** allow renku run with many inputs ([f60783e](https://github.com/SwissDataScienceCenter/renku-python/commit/f60783edd681deabe944d21997891186575a0b47)), closes [#552](https://github.com/SwissDataScienceCenter/renku-python/issues/552)
* added check for overwriting datasets ([#541](https://github.com/SwissDataScienceCenter/renku-python/issues/541)) ([8c697fb](https://github.com/SwissDataScienceCenter/renku-python/commit/8c697fbd4205ee7d1b22896e6f32b1b4476d7297))
* escape whitespaces in notebook name ([#584](https://github.com/SwissDataScienceCenter/renku-python/issues/584)) ([0542fcc](https://github.com/SwissDataScienceCenter/renku-python/commit/0542fcc9c9d66eae50ca73f68d9c91c582b20a89))
* modify json-ld for datasets ([#534](https://github.com/SwissDataScienceCenter/renku-python/issues/534)) ([ab6a719](https://github.com/SwissDataScienceCenter/renku-python/commit/ab6a719ef2c935ce5a20b6d76540d8fdec0d9bee)), closes [#525](https://github.com/SwissDataScienceCenter/renku-python/issues/525) [#526](https://github.com/SwissDataScienceCenter/renku-python/issues/526)
* refactored tests and docs to align with updated pydoctstyle ([#586](https://github.com/SwissDataScienceCenter/renku-python/issues/586)) ([6f981c8](https://github.com/SwissDataScienceCenter/renku-python/commit/6f981c835454612e8e5290970df58c7dd7c73727))
* **cli:** add check of missing references ([9a373da](https://github.com/SwissDataScienceCenter/renku-python/commit/9a373dafb8ec138c3608ed5e6e03cea0300492f1))
* **cli:** fail when removing non existing dataset ([dd728db](https://github.com/SwissDataScienceCenter/renku-python/commit/dd728db4fc196f8175190eb638a4153e05e0a6af))
* **status:** fix renku status output when not in root folder ([#564](https://github.com/SwissDataScienceCenter/renku-python/issues/564)) ([873270d](https://github.com/SwissDataScienceCenter/renku-python/commit/873270d4747f16d29317814c06da0ef6b1da9aa7)), closes [#551](https://github.com/SwissDataScienceCenter/renku-python/issues/551)
* added dependencies for SSL support ([#565](https://github.com/SwissDataScienceCenter/renku-python/issues/565)) ([4fa0fed](https://github.com/SwissDataScienceCenter/renku-python/commit/4fa0fedaa7753c8143be88d665302bd8ffad6d30))
* **datasets:** strip query string from data filenames ([450898b](https://github.com/SwissDataScienceCenter/renku-python/commit/450898b2fab59781e91a44b267903db1bb5d19a1))
* fixed serialization of creators ([#550](https://github.com/SwissDataScienceCenter/renku-python/issues/550)) ([6a9173c](https://github.com/SwissDataScienceCenter/renku-python/commit/6a9173cc01b262ca0365cee061fc7550647120a6))
* updated docs ([#539](https://github.com/SwissDataScienceCenter/renku-python/issues/539)) ([ff9a67c](https://github.com/SwissDataScienceCenter/renku-python/commit/ff9a67c4badd45beed15e30d812e988821a784e3))
* **cli:** remove dataset aliases ([6206e62](https://github.com/SwissDataScienceCenter/renku-python/commit/6206e6293283d1435913229de8f1874fc804e6c3))
* **cwl:** detect script as input parameter ([e23b75a](https://github.com/SwissDataScienceCenter/renku-python/commit/e23b75aceb33480d36ca5297f05cebba92e17bd2)), closes [#495](https://github.com/SwissDataScienceCenter/renku-python/issues/495)
* **deps:** updated dependencies ([691644d](https://github.com/SwissDataScienceCenter/renku-python/commit/691644d695b055a01e0ca22b2620e55bbd928c0d))


### Features

* add dataset metadata to the KG ([#558](https://github.com/SwissDataScienceCenter/renku-python/issues/558)) ([fb443d7](https://github.com/SwissDataScienceCenter/renku-python/commit/fb443d752436478a2903c4403ba6c702949b1b59))
* **datasets:** export dataset to zenodo ([#529](https://github.com/SwissDataScienceCenter/renku-python/issues/529)) ([fc6fd4f](https://github.com/SwissDataScienceCenter/renku-python/commit/fc6fd4fd01807f303fde23af931a3aa2a61b7561))
* added support for working on dirty repo ([ae67be7](https://github.com/SwissDataScienceCenter/renku-python/commit/ae67be74587a181e6a3c83cdf07ecbc082e58a06))
* **datasets:** edit dataset metadata ([#549](https://github.com/SwissDataScienceCenter/renku-python/issues/549)) ([db39083](https://github.com/SwissDataScienceCenter/renku-python/commit/db39083321fce8f653fe4d785657ac79150519e2))
* integrate metadata from zenodo ([#545](https://github.com/SwissDataScienceCenter/renku-python/issues/545)) ([4273d2a](https://github.com/SwissDataScienceCenter/renku-python/commit/4273d2aa962aa5b3b863dfa91b0c0431f97909d2))
* **config:** added global config manager ([#533](https://github.com/SwissDataScienceCenter/renku-python/issues/533)) ([938f820](https://github.com/SwissDataScienceCenter/renku-python/commit/938f82005e10c6d4365793c81b9592cbdfe3ba67))
* **datasets:** import data from zenodo ([#509](https://github.com/SwissDataScienceCenter/renku-python/issues/509)) ([52b2769](https://github.com/SwissDataScienceCenter/renku-python/commit/52b2769234e419a2484a5838662e5eafc864b423))



# [0.5.0](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.4.0...v0.5.0) (2019-03-28)


### Bug Fixes

* **api:** make methods lock free ([1f63964](https://github.com/SwissDataScienceCenter/renku-python/commit/1f6396497a8c1776c6adc3555a8ce45a3b457149)), closes [#486](https://github.com/SwissDataScienceCenter/renku-python/issues/486)
* use safe_load for parsing yaml ([5383d1e](https://github.com/SwissDataScienceCenter/renku-python/commit/5383d1ebb15ce7c93b1314014031888bf714b51b)), closes [#464](https://github.com/SwissDataScienceCenter/renku-python/issues/464)
* **datasets:** link flag on dataset add ([eae30f4](https://github.com/SwissDataScienceCenter/renku-python/commit/eae30f49a261706383d44b56d26b7e084c34d7f4))


### Features

* **api:** list datasets from a commit ([04a9fe9](https://github.com/SwissDataScienceCenter/renku-python/commit/04a9fe9953e0290bcd59b0fc8ce1dbec69ad06f4))
* **cli:** add dataset rm command ([a70c7ce](https://github.com/SwissDataScienceCenter/renku-python/commit/a70c7ce704439e5d694b223b3c0f0a1f157872db))
* **cli:** add rm command ([cf0f502](https://github.com/SwissDataScienceCenter/renku-python/commit/cf0f50271045ae6382bb7cd32a34da7d3f1051bc))
* **cli:** configurable format of dataset output ([d37abf3](https://github.com/SwissDataScienceCenter/renku-python/commit/d37abf38795d5a49c8211c6b3be205cbbc10c91f))
* **dataset:** add existing file from current repo ([575686b](https://github.com/SwissDataScienceCenter/renku-python/commit/575686bd81e9b16f57f9e80d7195332897f0dd21)), closes [#99](https://github.com/SwissDataScienceCenter/renku-python/issues/99)
* **datasets:** added ls-files command ([ccc4f59](https://github.com/SwissDataScienceCenter/renku-python/commit/ccc4f59b23483f5ecc83f85643232cff62c558cc))
* **models:** reference context for relative paths ([5d1e8e7](https://github.com/SwissDataScienceCenter/renku-python/commit/5d1e8e7e0f8c807a0ea2382bf038f9b1e549b0d1)), closes [#452](https://github.com/SwissDataScienceCenter/renku-python/issues/452)
* add JSON-LD output format for datasets ([c755d7b](https://github.com/SwissDataScienceCenter/renku-python/commit/c755d7bc6fce0d84c02447b22e9346a1e0670d48)), closes [#426](https://github.com/SwissDataScienceCenter/renku-python/issues/426)
* generate Makefile with log --format Makefile ([1e440ce](https://github.com/SwissDataScienceCenter/renku-python/commit/1e440ce4861b8c0d9d415b3aaf55b095842677ef))



# [0.4.0](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.3.3...v0.4.0) (2019-03-05)


### Bug Fixes

* **cli:** correctly move symlinked file ([27f9a7c](https://github.com/SwissDataScienceCenter/renku-python/commit/27f9a7c8fc54d60c253546c77a84076a6426f0cc))
* pull LFS objects from submodules correctly ([7cd8d40](https://github.com/SwissDataScienceCenter/renku-python/commit/7cd8d40d56dd76f439aa57d279b243ced064baf7))
* **cli:** lazy lfs pull before running commands ([32de730](https://github.com/SwissDataScienceCenter/renku-python/commit/32de730cb42d9608c62243c463d378f80b479a98)), closes [#332](https://github.com/SwissDataScienceCenter/renku-python/issues/332)
* removes typo causing wrong path resolution ([ee71eff](https://github.com/SwissDataScienceCenter/renku-python/commit/ee71eff955e6440d7a4826639ca0c937acc71cc4))
* **compat:** support strict param in path.resolve ([9344330](https://github.com/SwissDataScienceCenter/renku-python/commit/9344330f5f84ce9e3e0aac69e9714462495f4f78))


### Features

* **cli:** add mv command ([4c49f05](https://github.com/SwissDataScienceCenter/renku-python/commit/4c49f05e81f66fbd48192bce69e5d1a7bc70563d)), closes [#341](https://github.com/SwissDataScienceCenter/renku-python/issues/341)
* **datasets:** use uuid for metadata path ([dc9507e](https://github.com/SwissDataScienceCenter/renku-python/commit/dc9507e6681d8ea78314acfb6e809919d0946553))


### Performance Improvements

* improve exception handling in Sentry ([035d065](https://github.com/SwissDataScienceCenter/renku-python/commit/035d0657bb33f4dba23bec61a9212a1531d932b4))
* report exceptions to Sentry when configured ([97f3c29](https://github.com/SwissDataScienceCenter/renku-python/commit/97f3c29724700e801dd7c38fc806ba2c5a1c0c8f))



## [0.3.3](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.3.2...v0.3.3) (2018-12-07)



## [0.3.2](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.3.1...v0.3.2) (2018-11-30)



## [0.3.1](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.3.0...v0.3.1) (2018-11-29)



# [0.3.0](https://github.com/SwissDataScienceCenter/renku-python/compare/v0.2.0...v0.3.0) (2018-11-26)



# 0.2.0 (2018-09-25)



