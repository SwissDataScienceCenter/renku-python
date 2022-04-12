# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
r"""Renku CLI commands for handling of datasets.

Manipulating datasets
~~~~~~~~~~~~~~~~~~~~~

.. image:: ../_static/asciicasts/dataset-create.delay.gif
   :width: 850
   :alt: Create a Dataset

Creating an empty dataset inside a Renku project:

.. code-block:: console

    $ renku dataset create my-dataset
    Creating a dataset ... OK

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset create <dataset>
   :description: Create a new dataset.
   :extended:

You can pass the following options to this command to set various metadata for
the dataset.

+-------------------+------------------------------------------------------+
| Option            | Description                                          |
+===================+======================================================+
| -t, --title       | A human-readable title for the dataset.              |
+-------------------+------------------------------------------------------+
| -d, --description | Dataset's description.                               |
+-------------------+------------------------------------------------------+
| -c, --creator     | Creator's name, email, and an optional affiliation.  |
|                   | Accepted format is                                   |
|                   | 'Forename Surname <email> [affiliation]'. Pass       |
|                   | multiple times for a list of creators.               |
+-------------------+------------------------------------------------------+
| -k, --keyword     | Dataset's keywords. Pass multiple times for a list   |
|                   | of keywords.                                         |
+-------------------+------------------------------------------------------+
| -m, --metadata    | Path to file containing custom JSON-LD metadata to   |
|                   | be added to the dataset.                             |
+-------------------+------------------------------------------------------+

Editing a dataset's metadata:

.. image:: ../_static/asciicasts/dataset-edit.delay.gif
   :width: 850
   :alt: Editing a Dataset

Use the ``edit`` sub-command to change metadata of a dataset. You can edit the same
set of metadata as the create command by passing the options described in the
table above.

.. code-block:: console

    $ renku dataset edit my-dataset --title 'New title'
    Successfully updated: title.

Listing all datasets:

.. code-block:: console

    $ renku dataset ls
    ID        NAME           TITLE          VERSION
    --------  -------------  -------------  ---------
    0ad1cb9a  some-dataset   Some Dataset
    9436e36c  my-dataset     My Dataset

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset ls
   :description: List all datasets in the project.
   :extended:

You can select which columns to display by using ``--columns`` to pass a
comma-separated list of column names:

.. code-block:: console

    $ renku dataset ls --columns id,name,date_created,creators
    ID        NAME           CREATED              CREATORS
    --------  -------------  -------------------  ---------
    0ad1cb9a  some-dataset   2020-03-19 16:39:46  sam
    9436e36c  my-dataset     2020-02-28 16:48:09  sam

Displayed results are sorted based on the value of the first column.

You can specify output formats by passing ``--format`` with a value of ``tabular``,
``json-ld`` or ``json``.

To inspect the state of the dataset on a given commit we can use ``--revision``
flag for it:

.. code-block:: console

    $ renku dataset ls --revision=1103a42bd3006c94ef2af5d6a5e03a335f071215
    ID        NAME                 TITLE               VERSION
    a1fd8ce2  201901_us_flights_1  2019-01 US Flights  1
    c2d80abe  ds1                  ds1

Showing dataset details:

.. code-block:: console

    $ renku dataset show some-dataset
    Name: some-dataset
    Created: 2020-12-09 13:52:06.640778+00:00
    Creator(s): John Doe<john.doe@example.com> [SDSC]
    Keywords: Dataset, Data
    Annotations:
    [
      {...}
    ]
    Title: Some Dataset
    Description:
    Just some dataset

Deleting a dataset:

.. code-block:: console

    $ renku dataset rm some-dataset
    OK


.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset rm <dataset>
   :description: Remove a dataset.
   :extended:


Working with data
~~~~~~~~~~~~~~~~~

.. image:: ../_static/asciicasts/dataset-add.delay.gif
   :width: 850
   :alt: Add data to a Dataset

Adding data to the dataset:

.. code-block:: console

    $ renku dataset add my-dataset http://data-url

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset add <dataset> <url>
   :description: Add data from <url> to a dataset. <url> can be a local
                 file path, an http(s) address or a Git git+http or git+ssh repository.
   :extended:

This will copy the contents of ``data-url`` to the dataset and add it
to the dataset metadata.

You can create a dataset when you add data to it for the first time by passing
``--create`` flag to add command:

.. code-block:: console

    $ renku dataset add --create new-dataset http://data-url

To add data from a git repository, you can specify it via https or git+ssh
URL schemes. For example,

.. code-block:: console

    $ renku dataset add my-dataset git+ssh://host.io/namespace/project.git

Sometimes you want to add just specific paths within the parent project.
In this case, use the ``--source`` or ``-s`` flag:

.. code-block:: console

    $ renku dataset add my-dataset --source path/within/repo/to/datafile \
        git+ssh://host.io/namespace/project.git

The command above will result in a structure like

.. code-block:: console

    data/
      my-dataset/
        datafile

You can use shell-like wildcards (e.g. *, **, ?) when specifying paths to be
added. Put wildcard patterns in quotes to prevent your shell from expanding
them.

.. code-block:: console

    $ renku dataset add my-dataset --source 'path/**/datafile' \
        git+ssh://host.io/namespace/project.git

You can use ``--destination`` or ``-d`` flag to set the location where the new
data is copied to. This location be will under the dataset's data directory and
will be created if does not exists.

.. code-block:: console

    $ renku dataset add my-dataset \
        --source path/within/repo/to/datafile \
        --destination new-dir/new-subdir \
        git+ssh://host.io/namespace/project.git

will yield:

.. code-block:: console

    data/
      my-dataset/
        new-dir/
          new-subdir/
            datafile

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset add <dataset> --source <path>
             [--destination <rel-path>] <git-url>
   :description: Add only data in <path> from Git. With --destination:
                 location the data is copied to.
   :extended:

To add a specific version of files, use ``--ref`` option for selecting a
branch, commit, or tag. The value passed to this option must be a valid
reference in the remote Git repository.

Adding external data to the dataset:

Sometimes you might want to add data to your dataset without copying the
actual files to your repository. This is useful for example when external data
is too large to store locally. The external data must exist (i.e. be mounted)
on your filesystem. Renku creates a symbolic to your data and you can use this
symbolic link in renku commands as a normal file. To add an external file pass
``--external`` or ``-e`` when adding local data to a dataset:

.. code-block:: console

    $ renku dataset add my-dataset -e /path/to/external/file

Updating a dataset:

After adding files from a remote Git repository or importing a dataset from a
provider like Dataverse or Zenodo, you can check for updates in those files by
using ``renku dataset update --all`` command. For Git repositories, this command
checks all remote files and copies over new content if there is any. It does
not delete files from the local dataset if they are deleted from the remote Git
repository; to force the delete use ``--delete`` argument. You can update to a
specific branch, commit, or tag by passing ``--ref`` option.
For datasets from providers like Dataverse or Zenodo, the whole dataset is
updated to ensure consistency between the remote and local versions. Due to
this limitation, the ``--include`` and ``--exclude`` flags are not compatible
with those datasets. Modifying those datasets locally will prevent them from
being updated.

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset update <dataset>
   :description: Update files in a dataset based on their source.
   :extended:

The update command also checks for file changes in the project and updates
datasets' metadata accordingly.

You can limit the scope of updated files by specifying dataset names, using
``--include`` and ``--exclude`` to filter based on file names, or using
``--creators`` to filter based on creators. For example, the following command
updates only CSV files from ``my-dataset``:

.. code-block:: console

    $ renku dataset update -I '*.csv' my-dataset

Note that putting glob patterns in quotes is needed to tell Unix shell not
to expand them.

External data are also updated automatically. Since they require a checksum
calculation which can take a long time when data is large, you can exclude them
from an update by passing ``--no-external`` flag to the update command:

.. code-block:: console

    $ renku dataset update --all --no-external

You can use ``--dry-run`` flag to get a preview of what files/datasets will be
updated by an update operation.

Tagging a dataset:

A dataset can be tagged with an arbitrary tag to refer to the dataset at that
point in time. A tag can be added like this:

.. code-block:: console

    $ renku dataset tag my-dataset 1.0 -d "Version 1.0 tag"

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset tag <dataset> <tag> [-d <desc>]
   :description: Add a tag to the current version of the dataset, with
                 description <desc>.
   :extended:

A list of all tags can be seen by running:

.. code-block:: console

    $ renku dataset ls-tags my-dataset
    CREATED              NAME    DESCRIPTION      DATASET     COMMIT
    -------------------  ------  ---------------  ----------  ----------------
    2020-09-19 17:29:13  1.0     Version 1.0 tag  my-dataset  6c19a8d31545b...

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset ls-tags <dataset>
   :description: List all tags for a dataset.
   :extended:


A tag can be removed with:

.. code-block:: console

    $ renku dataset rm-tags my-dataset 1.0

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset rm-tags <dataset> <tags...>
   :description: Remove tags from a dataset.
   :extended:


Importing data from other Renku projects:

To import all data files and their metadata from another Renku dataset use:

.. code-block:: console

    $ renku dataset import \
        https://renkulab.io/projects/<username>/<project>/datasets/<dataset-id>

or

.. code-block:: console

    $ renku dataset import \
        https://renkulab.io/datasets/<dataset-id>

You can get the link to a dataset form the UI or you can construct it by
knowing the dataset's ID.


Importing data from an external provider:

.. image:: ../_static/asciicasts/dataset-import.delay.gif
   :width: 850
   :alt: Import a Dataset

.. code-block:: console

    $ renku dataset import 10.5281/zenodo.3352150

This will import the dataset with the DOI (Digital Object Identifier)
``10.5281/zenodo.3352150`` and make it locally available.
Dataverse and Zenodo are supported, with DOIs (e.g. ``10.5281/zenodo.3352150``
or ``doi:10.5281/zenodo.3352150``) and full URLs (e.g.
``http://zenodo.org/record/3352150``). A tag with the remote version of the
dataset is automatically created.

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset import <uri>
   :description: Import a dataset. <uri> can be a Renku, Zenodo or Dataverse
                 URL or DOI.
   :extended:

Exporting data to an external provider:

.. code-block:: console

    $ renku dataset export my-dataset zenodo

This will export the dataset ``my-dataset`` to ``zenodo.org`` as a draft,
allowing for publication later on. If the dataset has any tags set, you
can chose if the repository `HEAD` version or one of the tags should be
exported. The remote version will be set to the local tag that is being
exported.

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset export <dataset> <provider>
   :description: Export the dataset <dataset> to <provider>. Providers:
                 Zenodo, Dataverse.
   :extended:

To export to a Dataverse provider you must pass Dataverse server's URL and
the name of the parent dataverse where the dataset will be exported to.
Server's URL is stored in your Renku setting and you don't need to pass it
every time.

To export a dataset to OLOS you must pass the OLOS server's base URL and
supply your access token when prompted for it. You must also choose which
organizational unit to export the dataset to from the list shown during
the export. The export does not map contributors from Renku to OLOS and
also doesn't map License information. Additionally, all file categories
default to Primary/Derived. This has to adjusted manually in the OLOS
interface after the export is done.


Listing all files in the project associated with a dataset.

.. code-block:: console

    $ renku dataset ls-files
    DATASET NAME         ADDED                PATH                           LFS
    -------------------  -------------------  -----------------------------  ----
    my-dataset           2020-02-28 16:48:09  data/my-dataset/add-me         *
    my-dataset           2020-02-28 16:49:02  data/my-dataset/weather/file1  *
    my-dataset           2020-02-28 16:49:02  data/my-dataset/weather/file2
    my-dataset           2020-02-28 16:49:02  data/my-dataset/weather/file3  *

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset ls-files
   :description: List all dataset files in project.
   :extended:

You can select which columns to display by using ``--columns`` to pass a
comma-separated list of column names:

.. code-block:: console

    $ renku dataset ls-files --columns name,creators, path
    DATASET NAME         CREATORS   PATH
    -------------------  ---------  -----------------------------
    my-dataset           sam        data/my-dataset/add-me
    my-dataset           sam        data/my-dataset/weather/file1
    my-dataset           sam        data/my-dataset/weather/file2
    my-dataset           sam        data/my-dataset/weather/file3

Displayed results are sorted based on the value of the first column.

You can specify output formats by passing ``--format`` with a value of ``tabular``,
``json-ld`` or ``json``.

Sometimes you want to filter the files. For this we use ``--dataset``,
``--include`` and ``--exclude`` flags:

.. code-block:: console

    $ renku dataset ls-files --include "file*" --exclude "file3"
    DATASET NAME        ADDED                PATH                           LFS
    ------------------- -------------------  -----------------------------  ----
    my-dataset          2020-02-28 16:49:02  data/my-dataset/weather/file1  *
    my-dataset          2020-02-28 16:49:02  data/my-dataset/weather/file2  *

Unlink a file from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset --include file1
    OK

.. cheatsheet::
   :group: Datasets
   :command: $ renku dataset unlink <dataset> [--include <path|pattern>]
   :description: Remove files from a dataset.
   :extended:

Unlink all files within a directory from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset --include "weather/*"
    OK

Unlink all files from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset
    Warning: You are about to remove following from "my-dataset" dataset.
    .../my-dataset/weather/file1
    .../my-dataset/weather/file2
    .../my-dataset/weather/file3
    Do you wish to continue? [y/N]:

.. note:: The ``unlink`` command does not delete files,
    only the dataset record.
"""

import json
from pathlib import Path

import click

import renku.ui.cli.utils.color as color
from renku.command.format.dataset_files import DATASET_FILES_COLUMNS, DATASET_FILES_FORMATS
from renku.command.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.command.format.datasets import DATASETS_COLUMNS, DATASETS_FORMATS
from renku.ui.cli.utils.callback import ClickCallback


def _complete_datasets(ctx, param, incomplete):
    from renku.command.dataset import search_datasets_command

    try:
        result = search_datasets_command().build().execute(name=incomplete)
        return result.output
    except Exception:
        return []


@click.group()
def dataset():
    """Dataset commands."""


@dataset.command("ls")
@click.option(
    "--format", type=click.Choice(list(DATASETS_FORMATS.keys())), default="tabular", help="Choose an output format."
)
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="id,name,title,version",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(DATASETS_COLUMNS.keys())),
    show_default=True,
)
def list_dataset(format, columns):
    """List datasets."""
    from renku.command.dataset import list_datasets_command

    if format not in DATASETS_FORMATS:
        raise click.BadParameter(f"format '{format}' not supported")

    result = list_datasets_command().lock_dataset().build().execute()

    click.echo(DATASETS_FORMATS[format](result.output, columns=columns))


@dataset.command()
@click.argument("name")
@click.option("-t", "--title", default=None, type=click.STRING, help="Title of the dataset.")
@click.option("-d", "--description", default=None, type=click.STRING, help="Dataset's description.")
@click.option(
    "-c",
    "--creator",
    "creators",
    default=None,
    multiple=True,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option(
    "-m",
    "--metadata",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Custom metadata to be associated with the dataset.",
)
@click.option("-k", "--keyword", default=None, multiple=True, type=click.STRING, help="List of keywords.")
def create(name, title, description, creators, metadata, keyword):
    """Create an empty dataset in the current repo."""
    from renku.command.dataset import create_dataset_command
    from renku.core.util.metadata import construct_creators

    communicator = ClickCallback()
    creators = creators or ()

    custom_metadata = None

    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    if creators:
        creators, _ = construct_creators(creators)

    result = (
        create_dataset_command()
        .with_communicator(communicator)
        .build()
        .execute(
            name=name,
            title=title,
            description=description,
            creators=creators,
            keywords=keyword,
            custom_metadata=custom_metadata,
        )
    )

    new_dataset = result.output

    click.echo(f'Use the name "{new_dataset.name}" to refer to this dataset.')
    click.secho("OK", fg=color.GREEN)


@dataset.command()
@click.argument("name", shell_complete=_complete_datasets)
@click.option("-t", "--title", default=None, type=click.STRING, help="Title of the dataset.")
@click.option("-d", "--description", default=None, type=click.STRING, help="Dataset's description.")
@click.option(
    "-c",
    "--creator",
    "creators",
    default=None,
    multiple=True,
    help="Creator's name, email, and affiliation. " "Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option(
    "-m",
    "--metadata",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Custom metadata to be associated with the dataset.",
)
@click.option("-k", "--keyword", default=None, multiple=True, type=click.STRING, help="List of keywords or tags.")
def edit(name, title, description, creators, metadata, keyword):
    """Edit dataset metadata."""
    from renku.command.dataset import edit_dataset_command
    from renku.core.util.metadata import construct_creators

    creators = creators or ()
    keywords = keyword or ()

    custom_metadata = None
    no_email_warnings = False

    if creators:
        creators, no_email_warnings = construct_creators(creators, ignore_email=True)

    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    updated = (
        edit_dataset_command()
        .build()
        .execute(
            name=name,
            title=title,
            description=description,
            creators=creators,
            keywords=keywords,
            skip_image_update=True,
            custom_metadata=custom_metadata,
        )
    ).output

    if not updated:
        click.echo(
            (
                "Nothing to update. "
                "Check available fields with `renku dataset edit --help`\n\n"
                'Hint: `renku dataset edit --title "new title"`'
            )
        )
    else:
        click.echo("Successfully updated: {}.".format(", ".join(updated.keys())))
        if no_email_warnings:
            click.echo(ClickCallback.WARNING + "No email or wrong format for: " + ", ".join(no_email_warnings))


@dataset.command("show")
@click.argument("name", shell_complete=_complete_datasets)
def show(name):
    """Show metadata of a dataset."""
    from renku.command.dataset import show_dataset_command
    from renku.ui.cli.utils.terminal import print_markdown

    result = show_dataset_command().build().execute(name=name)
    ds = result.output

    click.echo(click.style("Name: ", bold=True, fg=color.MAGENTA) + click.style(ds["name"], bold=True))
    click.echo(click.style("Created: ", bold=True, fg=color.MAGENTA) + (ds.get("created_at", "") or ""))

    creators = []
    for creator in ds.get("creators", []):
        if creator["affiliation"]:
            creators.append(f"{creator['name']} <{creator['email']}> [{creator['affiliation']}]")
        else:
            creators.append(f"{creator['name']} <{creator['email']}>")

    click.echo(click.style("Creator(s): ", bold=True, fg=color.MAGENTA) + ", ".join(creators))
    if ds["keywords"]:
        click.echo(click.style("Keywords: ", bold=True, fg=color.MAGENTA) + ", ".join(ds.get("keywords", "")))

    if ds["version"]:
        click.echo(click.style("Version: ", bold=True, fg=color.MAGENTA) + ds.get("version", ""))

    if ds["annotations"]:
        click.echo(click.style("Annotations: ", bold=True, fg=color.MAGENTA))
        click.echo(json.dumps(ds.get("annotations", ""), indent=2))

    click.echo(click.style("Title: ", bold=True, fg=color.MAGENTA) + click.style(ds.get("title", ""), bold=True))

    click.echo(click.style("Description: ", bold=True, fg=color.MAGENTA))
    print_markdown(ds.get("description", "") or "")


@dataset.command()
@click.argument("name", shell_complete=_complete_datasets)
@click.argument("urls", type=click.Path(), nargs=-1)
@click.option("-e", "--external", is_flag=True, help="Creates a link to external data.")
@click.option("--force", is_flag=True, help="Allow adding otherwise ignored files.")
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing files.")
@click.option("-c", "--create", is_flag=True, help="Create dataset if it does not exist.")
@click.option(
    "-s", "--src", "--source", "sources", default=None, multiple=True, help="Path(s) within remote git repo to be added"
)
@click.option(
    "-d", "--dst", "--destination", "destination", default="", help="Destination directory within the dataset path"
)
@click.option("--ref", default=None, help="Add files from a specific commit/tag/branch.")
def add(name, urls, external, force, overwrite, create, sources, destination, ref):
    """Add data to a dataset."""
    from renku.command.dataset import add_to_dataset_command

    communicator = ClickCallback()
    add_to_dataset_command().with_communicator(communicator).build().execute(
        urls=urls,
        dataset_name=name,
        external=external,
        force=force,
        overwrite=overwrite,
        create=create,
        sources=sources,
        destination=destination,
        ref=ref,
    )
    click.secho("OK", fg=color.GREEN)


@dataset.command("ls-files")
@click.argument("names", nargs=-1, shell_complete=_complete_datasets)
@click.option(
    "--creators",
    help="Filter files which where authored by specific creators. Multiple creators are specified by comma.",
)
@click.option("-I", "--include", default=None, multiple=True, help="Include files matching given pattern.")
@click.option("-X", "--exclude", default=None, multiple=True, help="Exclude files matching given pattern.")
@click.option(
    "--format",
    type=click.Choice(list(DATASET_FILES_FORMATS.keys())),
    default="tabular",
    help="Choose an output format.",
)
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="dataset_name,added,size,path,lfs",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(DATASET_FILES_COLUMNS.keys())),
    show_default=True,
)
def ls_files(names, creators, include, exclude, format, columns):
    """List files in dataset."""
    from renku.command.dataset import list_files_command

    if format not in DATASETS_FORMATS:
        raise click.BadParameter(f"Format '{format}' not supported")

    result = (
        list_files_command()
        .lock_dataset()
        .build()
        .execute(datasets=names, creators=creators, include=include, exclude=exclude)
    )

    click.echo(DATASET_FILES_FORMATS[format](result.output, columns=columns))


@dataset.command()
@click.argument("name", shell_complete=_complete_datasets)
@click.option("-I", "--include", multiple=True, help="Include files matching given pattern.")
@click.option("-X", "--exclude", multiple=True, help="Exclude files matching given pattern.")
@click.option("-y", "--yes", is_flag=True, help="Confirm unlinking of all files.")
def unlink(name, include, exclude, yes):
    """Remove matching files from a dataset."""
    from renku.command.dataset import file_unlink_command
    from renku.core import errors

    if not include and not exclude:
        raise errors.ParameterError(
            (
                "include or exclude filters not found.\n"
                "Check available filters with 'renku dataset unlink --help'\n"
                "Hint: 'renku dataset unlink my-dataset -I path'"
            )
        )

    communicator = ClickCallback()
    file_unlink_command().with_communicator(communicator).build().execute(
        name=name, include=include, exclude=exclude, yes=yes
    )
    click.secho("OK", fg=color.GREEN)


@dataset.command("rm")
@click.argument("name")
def remove(name):
    """Delete a dataset."""
    from renku.command.dataset import remove_dataset_command

    remove_dataset_command().build().execute(name)
    click.secho("OK", fg=color.GREEN)


@dataset.command("tag")
@click.argument("name", shell_complete=_complete_datasets)
@click.argument("tag")
@click.option("-d", "--description", default="", help="A description for this tag")
@click.option("--force", is_flag=True, help="Allow overwriting existing tags.")
def tag(name, tag, description, force):
    """Create a tag for a dataset."""
    from renku.command.dataset import add_dataset_tag_command

    add_dataset_tag_command().build().execute(dataset_name=name, tag=tag, description=description, force=force)
    click.secho("OK", fg=color.GREEN)


@dataset.command("rm-tags")
@click.argument("name", shell_complete=_complete_datasets)
@click.argument("tags", nargs=-1)
def remove_tags(name, tags):
    """Remove tags from a dataset."""
    from renku.command.dataset import remove_dataset_tags_command

    remove_dataset_tags_command().build().execute(dataset_name=name, tags=tags)
    click.secho("OK", fg=color.GREEN)


@dataset.command("ls-tags")
@click.argument("name", shell_complete=_complete_datasets)
@click.option(
    "--format", type=click.Choice(list(DATASET_TAGS_FORMATS.keys())), default="tabular", help="Choose an output format."
)
def ls_tags(name, format):
    """List all tags of a dataset."""
    from renku.command.dataset import list_tags_command

    result = list_tags_command().lock_dataset().build().execute(dataset_name=name, format=format)
    click.echo(result.output)


def export_provider_argument(*param_decls, **attrs):
    """Sets dataset export provider argument on the dataset export command."""

    def wrapper(f):
        from click import argument

        from renku.core.dataset.providers import ProviderFactory

        providers = [k.lower() for k, p in ProviderFactory.providers().items() if p.supports_export]
        f = argument("provider", type=click.Choice(providers))(f)
        return f

    return wrapper


def export_provider_options(*param_decls, **attrs):
    """Sets dataset export provider option groups on the dataset export command."""

    def wrapper(f):
        from click_option_group import optgroup

        from renku.core.dataset.providers import ProviderFactory

        providers = [
            (k, v) for k, v in ProviderFactory.providers().items() if v.supports_export and v.export_parameters()
        ]

        for i, (name, provider) in enumerate(providers):
            params = provider.export_parameters()

            for j, (param_name, (param_description, param_type)) in enumerate(params.items()):
                if j == 0:
                    param_description = f"\b\n{param_description}\n "  # NOTE: add newline after a group
                f = optgroup.option(f"--{param_name}", type=param_type, help=param_description)(f)

            name = f"{name} configuration"
            if i == len(providers) - 1:
                name = "\n  " + name  # NOTE: add newline before first group

            f = optgroup.group(name=name)(f)

        return f

    return wrapper


@dataset.command("export")
@click.argument("name", shell_complete=_complete_datasets)
@export_provider_argument()
@click.option("-p", "--publish", is_flag=True, help="Automatically publish exported dataset.")
@click.option("-t", "--tag", help="Dataset tag to export")
@export_provider_options()
def export_(name, provider, publish, tag, **kwargs):
    """Export data to 3rd party provider."""
    from renku.command.dataset import export_dataset_command
    from renku.core import errors

    try:
        communicator = ClickCallback()
        export_dataset_command().lock_dataset().with_communicator(communicator).build().execute(
            name=name, provider_name=provider, publish=publish, tag=tag, **kwargs
        )
    except (ValueError, errors.InvalidAccessToken, errors.DatasetNotFound, errors.RequestError) as e:
        raise click.BadParameter(e)

    click.secho("OK", fg=color.GREEN)


@dataset.command("import")
@click.argument("uri")
@click.option("--short-name", "--name", "name", default=None, help="A convenient name for dataset.")
@click.option("-x", "--extract", is_flag=True, help="Extract files before importing to dataset.")
@click.option("-y", "--yes", is_flag=True, help="Bypass download confirmation.")
def import_(uri, name, extract, yes):
    """Import data from a 3rd party provider or another renku project.

    Supported providers: [Dataverse, Renku, Zenodo]
    """
    from renku.command.dataset import import_dataset_command

    communicator = ClickCallback()
    import_dataset_command().with_communicator(communicator).build().execute(
        uri=uri, name=name, extract=extract, yes=yes
    )

    click.secho(" " * 79 + "\r", nl=False)
    click.secho("OK", fg=color.GREEN)


@dataset.command("update")
@click.argument("names", nargs=-1, shell_complete=_complete_datasets)
@click.option(
    "--creators",
    help="Filter files which where authored by specific creators. Multiple creators are specified by comma.",
)
@click.option("-I", "--include", default=None, multiple=True, help="Include files matching given pattern.")
@click.option("-X", "--exclude", default=None, multiple=True, help="Exclude files matching given pattern.")
@click.option("--ref", default=None, help="Update to a specific commit/tag/branch.")
@click.option("--delete", is_flag=True, help="Delete local files that are deleted from remote.")
@click.option("-e", "--external", is_flag=True, help="Deprecated")
@click.option("--no-external", is_flag=True, help="Skip updating external data.")
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all datasets.")
@click.option("-n", "--dry-run", is_flag=True, help="Show what would have been changed")
def update(names, creators, include, exclude, ref, delete, external, no_external, update_all, dry_run):
    """Updates files in dataset from a remote Git repo."""
    from renku.command.dataset import update_datasets_command
    from renku.core import errors

    communicator = ClickCallback()

    if external and no_external:
        raise errors.ParameterError("Cannot pass both '--external' and '--no-external'")
    elif external:
        communicator.warn("'-e/--external' argument is deprecated")

    if not update_all and not names and not include and not exclude and not dry_run:
        raise errors.ParameterError("Either NAMES, -a/--all, -n/--dry-run, or --include/--exclude should be specified")

    if names and update_all:
        raise errors.ParameterError("Cannot pass dataset names with -a/--all")
    elif (include or exclude) and update_all:
        raise errors.ParameterError("Cannot pass --include/--exclude with -a/--all")

    result = (
        update_datasets_command()
        .with_communicator(communicator)
        .build()
        .execute(
            names=list(names),
            creators=creators,
            include=include,
            exclude=exclude,
            ref=ref,
            delete=delete,
            no_external=no_external,
            update_all=update_all,
            dry_run=dry_run,
        )
    )

    if dry_run:
        datasets, dataset_files = result.output

        def get_dataset_files(records):
            from renku.command.format.tabulate import tabulate

            columns = {"path": ("path", None), "dataset": ("dataset.name", "dataset"), "external": ("external", None)}
            return tabulate(collection=records, columns="path,dataset,external", columns_mapping=columns)

        if not datasets and not dataset_files:
            click.secho("Everything is up-to-date", fg=color.GREEN)
            return

        if datasets:
            names = "\n\t".join(sorted([d.name for d in datasets]))
            click.echo(f"The following imported datasets will be updated:\n\t{names}\n")

        if not dataset_files:
            return

        files = [f for f in dataset_files if not f.deleted]
        if files:
            files = get_dataset_files(files)
            click.echo(f"The following files will be updated:\n\n{files}\n")

        deleted_files = [f for f in dataset_files if f.deleted]
        if deleted_files:
            files = get_dataset_files(deleted_files)
            message = " (pass '--delete' to remove them from datasets' metadata)" if not delete else ""
            click.echo(f"The following files will be deleted{message}:\n\n{files}\n")
