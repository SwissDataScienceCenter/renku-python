# renku-project-template
A repository to hold template files for new Renku projects to be used on project
creation by the Renku clients. The next sections outline what different files in
the template are used for.


## For running interactive environments from Renkulab

`Dockerfile` - File for building a docker image that you can launch from renkulab,
to work on your project in the cloud. Template-supplied contents will allow you to
launch an interactive environment from renkulab, with pre-installed renku CLI and
software dependencies that you put into your `requirements.txt`, `environment.yml`,
or `install.R`. You can and should add to this `Dockerfile` if libraries you install
require linux software installations as well; for more information see:
 https://github.com/SwissDataScienceCenter/renkulab-docker.

`.gitlab-ci.yml` - Configuration for running gitlab CI, which builds a docker image
out of the project on `git push` to renkulab so that you can launch your interactive
 environment (don't remove, but you can modify to add extra CI functionality).

`.dockerignore` - Files and directories to be excluded from docker build (you can
  append to this list); https://docs.docker.com/engine/reference/builder/#dockerignore-file.


## For managing software dependencies

`requirements.txt` - Required by template's Dockerfile; add your python pip
dependencies here.

`environment.yml` - Required by template's Dockerfile; add your python conda
dependencies here.

`install.R` - Required by template's Dockerfile (for r-based projects only).

## For the landing page for your project

`README.md` - Edit this file to provide information about your own project.
Initial contents explain how to use a renku project.

## For renku CLI

`.renku` - Directory containing renku metadata that renku commands update
(caution: don't update this manually).

`.renkulfsignore` - File similar to [.gitignore](https://git-scm.com/docs/gitignore)
for telling renku to NOT store listed files in git LFS. Use in conjunction with
`renku config lfs_threshold <[size]kb>` to tell renku to NOT store files above a
threshold size in LFS. Initial configuration is set to 100kb.

By default, `renku` commands (like `renku run` and `renku dataset`) store all output
files above a configurable threshold size of 100KB in [git LFS](https://git-lfs.github.com/)
to prevent accidentally committing large files to git. It's bad to git commit
large files (e.g. datasets, graphics, videos, audio samples) without being tracked
by git LFS, because they slow down git commands (and thus renku commands). However,
sometimes:

* an imported dataset will come with markdown (`*.md`) and/or code (e.g. `*.py`).
* a code file (like `*.ipynb`) will be generated from
  a `renku run` (e.g. with [papermill](https://papermill.readthedocs.io/en/latest/)).
* generated or imported data could be small (e.g. <100kb)

Tracking files with LFS is good, but limits your ability to use commands like
`git diff` to view changes, and to see the contents of the files in the project's
page on renkulab.

Thus, you can edit `.renkulfsignore` to add files with particular paths or extensions
that are relevant for your project. `renku` commands will consult `.renkulfsignore`
and not track those files with git LFS.

Note: When you start a new interactive environment, by default the LFS-tracked
files (e.g. files above the configured threshold AND not on this list) are in
their "pointer" form. Run `renku storage pull <filepath>` to pull the real content
into each file, or `git lfs pull` to replace all pointers with real content all
at once. Since these are large files, you might be better off pulling them one at
a time.

## For organizing project files

`data` - Initially empty directory where `renku dataset` creates subdirectories
for your named datasets and the files you add to those datasets (if you haven't
or will not be creating renku datasets, you can remove this directory).

`notebooks` - Initially empty directory to help you organize jupyter notebooks
(not a requirement, you can remove this directory).

## For git to ignore

`.gitignore` - Files and directories to be excluded from git repository (this
  template only requires the #renku section, but the others are nice-to-haves
  for common paths to ignore).
