# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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

Creating an empty dataset inside a Renku project:

.. code-block:: console

    $ renku dataset create my-dataset
    Creating a dataset ... OK

Listing all datasets:

.. code-block:: console

    $ renku dataset
    ID        NAME           CREATED              CREATORS
    --------  -------------  -------------------  ---------
    0ad1cb9a  some-dataset   2019-03-19 16:39:46  sam
    9436e36c  my-dataset     2019-02-28 16:48:09  sam

Deleting a dataset:

.. code-block:: console

    $ renku dataset rm some-dataset
    OK


Working with data
~~~~~~~~~~~~~~~~~


Adding data to the dataset:

.. code-block:: console

    $ renku dataset add my-dataset http://data-url

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

Sometimes you want to import just specific paths within the parent project.
In this case, use the ``--source`` or ``-s`` flag:

.. code-block:: console

    $ renku dataset add my-dataset --source path/within/repo/to/datafile \
        git+ssh://host.io/namespace/project.git

The command above will result in a structure like

.. code-block:: console

    data/
      my-dataset/
        datafile

You can use ``--destination`` or ``-d`` flag to change the name of the target
file or directory. The semantics here are similar to the POSIX copy command:
if the destination does not exist or if it is a file then the source will be
renamed; if the destination exists and is a directory the source will be copied
to it. You will get an error message if you try to move a directory to a file
or copy multiple files into one.

.. code-block:: console

    $ renku dataset add my-dataset \
        --source path/within/repo/to/datafile \
        --destination new-dir/new-filename \
        git+ssh://host.io/namespace/project.git

will yield:

.. code-block:: console

    data/
      my-dataset/
        new-dir/
          new-filename

To add a specific version of files, use ``--ref`` option for selecting a
branch, commit, or tag. The value passed to this option must be a valid
reference in the remote Git repository.

Updating a dataset:

After adding files from a remote Git repository, you can check for updates in
those files by using ``renku dataset update`` command. This command checks all
remote files and copies over new content if there is any. It does not delete
files from the local dataset if they are deleted from the remote Git
repository; to force the delete use ``--delete`` argument. You can update to a
specific branch, commit, or tag by passing ``--ref`` option.

You can limit the scope of updated files by specifying dataset names, using
``--include`` and ``--exclude`` to filter based on file names, or using
``--creators`` to filter based on creators. For example, the following command
updates only CSV files from ``my-dataset``:

.. code-block:: console

    $ renku dataset update -I '*.csv' my-dataset

Note that putting glob patterns in quotes is needed to tell Unix shell not
to expand them.

Tagging a dataset:

A dataset can be tagged with an arbitrary tag to refer to the dataset at that
point in time. A tag can be added like this:

.. code-block:: console

    $ renku dataset tag my-dataset 1.0 -d "Version 1.0 tag"

A list of all tags can be seen by running:

.. code-block:: console

    $ renku dataset ls-tags my-dataset
    CREATED              NAME    DESCRIPTION      DATASET     COMMIT
    -------------------  ------  ---------------  ----------  ----------------
    2019-09-19 17:29:13  1.0     Version 1.0 tag  my-dataset  6c19a8d31545b...


A tag can be removed with:

.. code-block:: console

    $ renku dataset rm-tags my-dataset 1.0



Importing data from an external provider:

.. code-block:: console

    $ renku dataset import 10.5281/zenodo.3352150

This will import the dataset with the DOI (Digital Object Identifier)
``10.5281/zenodo.3352150`` and make it locally available.
Dataverse and Zenodo are supported, with DOIs (e.g. ``10.5281/zenodo.3352150``
or ``doi:10.5281/zenodo.3352150``) and full URLs (e.g.
``http://zenodo.org/record/3352150``). A tag with the remote version of the
dataset is automatically created.

Exporting data to an external provider:

.. code-block:: console

    $ renku dataset export my-dataset zenodo

This will export the dataset ``my-dataset`` to ``zenodo.org`` as a draft,
allowing for publication later on. If the dataset has any tags set, you
can chose if the repository `HEAD` version or one of the tags should be
exported. The remote version will be set to the local tag that is being
exported.


Listing all files in the project associated with a dataset.

.. code-block:: console

    $ renku dataset ls-files
    ADDED                CREATORS    DATASET        PATH
    -------------------  ---------  -------------  ---------------------------
    2019-02-28 16:48:09  sam        my-dataset     ...my-dataset/addme
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file1
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file2
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file3

Sometimes you want to filter the files. For this we use ``--dataset``,
``--include`` and ``--exclude`` flags:

.. code-block:: console

    $ renku dataset ls-files --include "file*" --exclude "file3"
    ADDED                CREATORS    DATASET     PATH
    -------------------  ---------  ----------  ----------------------------
    2019-02-28 16:49:02  sam        my-dataset  .../my-dataset/weather/file1
    2019-02-28 16:49:02  sam        my-dataset  .../my-dataset/weather/file2

Unlink a file from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset --include file1
    OK

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
import multiprocessing as mp
import os
from functools import partial
from pathlib import Path
from time import sleep

import click
import editor
import requests
import yaml
from tqdm import tqdm

from renku.core.commands.dataset import add_file, create_dataset, \
    dataset_parent, dataset_remove, edit_dataset, export_dataset, \
    file_unlink, import_dataset, list_files, list_tags, remove_dataset_tags, \
    tag_dataset_with_client, update_datasets
from renku.core.commands.echo import WARNING, echo_via_pager, progressbar
from renku.core.commands.format.dataset_files import DATASET_FILES_FORMATS
from renku.core.commands.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core.commands.format.datasets import DATASETS_FORMATS
from renku.core.errors import DatasetNotFound, InvalidAccessToken


def prompt_access_token(exporter):
    """Prompt user for an access token for a provider.

    :return: The new access token
    """
    text_prompt = ('You must configure an access token\n')
    text_prompt += 'Create one at: {0}\n'.format(exporter.access_token_url())
    text_prompt += 'Access token'

    return click.prompt(text_prompt, type=str)


def prompt_tag_selection(tags):
    """Prompt user to chose a tag or <HEAD>."""
    # Prompt user to select a tag to export
    tags = sorted(tags, key=lambda t: t.created)

    text_prompt = 'Tag to export: \n\n<HEAD>\t[1]\n'

    text_prompt += '\n'.join(
        '{}\t[{}]'.format(t.name, i) for i, t in enumerate(tags, start=2)
    )

    text_prompt += '\n\nTag'
    selection = click.prompt(
        text_prompt, type=click.IntRange(1,
                                         len(tags) + 1), default=1
    )

    if selection > 1:
        return tags[selection - 2]
    return None


def download_file_with_progress(extract, data_folder, file, chunk_size=16384):
    """Download a file with progress tracking."""
    global current_process_position

    local_filename = Path(file.filename).name
    download_to = Path(data_folder) / Path(local_filename)

    def extract_dataset(data_folder_, filename):
        """Extract downloaded dataset."""
        import patoolib
        filepath = Path(data_folder_) / Path(filename)
        patoolib.extract_archive(filepath, outdir=data_folder_)
        filepath.unlink()

    def stream_to_file(request):
        """Stream bytes to file."""
        with open(str(download_to), 'wb') as f_:
            scaling_factor = 1e-6
            unit = 'MB'

            # We round sizes to 0.1, files smaller than 1e5 would
            # get rounded to 0, so we display bytes instead
            if file.filesize < 1e5:
                scaling_factor = 1.0
                unit = 'B'

            total = round(file.filesize * scaling_factor, 1)
            progressbar_ = tqdm(
                total=total,
                position=current_process_position,
                desc=file.filename[:32],
                bar_format=(
                    '{{percentage:3.0f}}% '
                    '{{n_fmt}}{unit}/{{total_fmt}}{unit}| '
                    '{{bar}} | {{desc}}'.format(unit=unit)
                ),
                leave=False,
            )

            try:
                bytes_downloaded = 0
                for chunk in request.iter_content(chunk_size=chunk_size):
                    if chunk:  # remove keep-alive chunks
                        f_.write(chunk)
                        bytes_downloaded += chunk_size
                        progressbar_.n = min(
                            float(
                                '{0:.1f}'.format(
                                    bytes_downloaded * scaling_factor
                                )
                            ), total
                        )
                        progressbar_.update(0)
            finally:
                sleep(0.1)
                progressbar_.close()

        if extract:
            extract_dataset(data_folder, local_filename)

    with requests.get(file.url.geturl(), stream=True) as r:
        r.raise_for_status()
        stream_to_file(r)


@click.group(invoke_without_command=True)
@click.option('--revision', default=None)
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@click.option(
    '--format',
    type=click.Choice(DATASETS_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
@click.pass_context
def dataset(ctx, revision, datadir, format):
    """Handle datasets."""
    if isinstance(ctx, click.Context):
        ctx.meta['renku.datasets.datadir'] = datadir

    if ctx.invoked_subcommand is not None:
        return

    click.echo(dataset_parent(revision, datadir, format, ctx=ctx))


@dataset.command()
@click.argument('name')
def create(name):
    """Create an empty dataset in the current repo."""
    create_dataset(name)
    click.secho('OK', fg='green')


@dataset.command()
@click.argument('dataset_id')
def edit(dataset_id):
    """Edit dataset metadata."""
    edit_dataset(
        dataset_id, lambda dataset: editor.edit(
            contents=bytes(yaml.safe_dump(dataset.editable), encoding='utf-8')
        )
    )


@dataset.command()
@click.argument('name')
@click.argument('urls', nargs=-1)
@click.option('--link', is_flag=True, help='Creates a hard link.')
@click.option(
    '--force', is_flag=True, help='Allow adding otherwise ignored files.'
)
@click.option(
    '--create', is_flag=True, help='Create dataset if it does not exist.'
)
@click.option(
    '-s',
    '--src',
    '--source',
    'sources',
    default=None,
    multiple=True,
    help='Path(s) within remote git repo to be added'
)
@click.option(
    '-d',
    '--dst',
    '--destination',
    'destination',
    default='',
    help='Destination file or directory within the dataset path'
)
@click.option(
    '--ref', default=None, help='Add files from a specific commit/tag/branch.'
)
def add(name, urls, link, force, create, sources, destination, ref):
    """Add data to a dataset."""
    progress = partial(progressbar, label='Adding data to dataset')
    add_file(
        urls=urls,
        name=name,
        link=link,
        force=force,
        create=create,
        sources=sources,
        destination=destination,
        ref=ref,
        urlscontext=progress
    )


@dataset.command('ls-files')
@click.argument('names', nargs=-1)
@click.option(
    '--creators',
    help='Filter files which where authored by specific creators. '
    'Multiple creators are specified by comma.'
)
@click.option(
    '-I',
    '--include',
    default=None,
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '-X',
    '--exclude',
    default=None,
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option(
    '--format',
    type=click.Choice(DATASET_FILES_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
def ls_files(names, creators, include, exclude, format):
    """List files in dataset."""
    echo_via_pager(list_files(names, creators, include, exclude, format))


@dataset.command()
@click.argument('name')
@click.option(
    '-I',
    '--include',
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '-X',
    '--exclude',
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option(
    '-y', '--yes', is_flag=True, help='Confirm unlinking of all files.'
)
def unlink(name, include, exclude, yes):
    """Remove matching files from a dataset."""
    with file_unlink(name, include, exclude) as records:
        if not yes and records:
            prompt_text = (
                'You are about to remove '
                'following from "{0}" dataset.\n'.format(dataset.name) +
                '\n'.join([str(record.full_path) for record in records]) +
                '\nDo you wish to continue?'
            )
            click.confirm(WARNING + prompt_text, abort=True)

    click.secho('OK', fg='green')


@dataset.command('rm')
@click.argument('names', nargs=-1)
def remove(names):
    """Delete a dataset."""
    datasetscontext = partial(
        progressbar,
        label='Removing metadata files'.ljust(30),
        item_show_func=lambda item: str(item) if item else ''
    )
    referencescontext = partial(
        progressbar,
        label='Removing aliases'.ljust(30),
        item_show_func=lambda item: item.name if item else '',
    )
    dataset_remove(
        names,
        with_output=True,
        datasetscontext=datasetscontext,
        referencescontext=referencescontext
    )
    click.secho('OK', fg='green')


@dataset.command('tag')
@click.argument('name')
@click.argument('tag')
@click.option(
    '-d', '--description', default='', help='A description for this tag'
)
@click.option('--force', is_flag=True, help='Allow overwriting existing tags.')
def tag(name, tag, description, force):
    """Create a tag for a dataset."""
    tag_dataset_with_client(name, tag, description, force)
    click.secho('OK', fg='green')


@dataset.command('rm-tags')
@click.argument('name')
@click.argument('tags', nargs=-1)
def remove_tags(name, tags):
    """Remove tags from a dataset."""
    remove_dataset_tags(name, tags)
    click.secho('OK', fg='green')


@dataset.command('ls-tags')
@click.argument('name')
@click.option(
    '--format',
    type=click.Choice(DATASET_TAGS_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
def ls_tags(name, format):
    """List all tags of a dataset."""
    tags_output = list_tags(name, format)
    click.echo(tags_output)


@dataset.command('export')
@click.argument('id')
@click.argument('provider')
@click.option(
    '-p',
    '--publish',
    is_flag=True,
    help='Automatically publish exported dataset.'
)
@click.option('-t', '--tag', help='Dataset tag to export')
def export_(id, provider, publish, tag):
    """Export data to 3rd party provider."""
    try:
        output = export_dataset(
            id,
            provider,
            publish,
            tag,
            handle_access_token_fn=prompt_access_token,
            handle_tag_selection_fn=prompt_tag_selection
        )
    except (
        ValueError, InvalidAccessToken, DatasetNotFound, requests.HTTPError
    ) as e:
        raise click.BadParameter(e)

    click.echo(output)
    click.secho('OK', fg='green')


@dataset.command('import')
@click.argument('uri')
@click.option('-n', '--name', help='Dataset name.')
@click.option(
    '-x',
    '--extract',
    is_flag=True,
    help='Extract files before importing to dataset.'
)
def import_(uri, name, extract):
    """Import data from a 3rd party provider.

    Supported providers: [Zenodo, Dataverse]
    """
    manager = mp.Manager()
    id_queue = manager.Queue()

    pool_size = min(int(os.getenv('RENKU_POOL_SIZE', mp.cpu_count() // 2)), 4)

    for i in range(pool_size):
        id_queue.put(i)

    def _init(lock, id_queue):
        """Set up tqdm lock and worker process index.

        See https://stackoverflow.com/a/42817946
        Fixes tqdm line position when |files| > terminal-height
        so only |workers| progressbars are shown at a time
        """
        global current_process_position
        current_process_position = id_queue.get()
        tqdm.set_lock(lock)

    import_dataset(
        uri,
        name,
        extract,
        with_prompt=True,
        pool_init_fn=_init,
        pool_init_args=(mp.RLock(), id_queue),
        download_file_fn=download_file_with_progress
    )
    click.secho('OK', fg='green')


@dataset.command('update')
@click.argument('names', nargs=-1)
@click.option(
    '--creators',
    help='Filter files which where authored by specific creators. '
    'Multiple creators are specified by comma.'
)
@click.option(
    '-I',
    '--include',
    default=None,
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '-X',
    '--exclude',
    default=None,
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option(
    '--ref', default=None, help='Update to a specific commit/tag/branch.'
)
@click.option(
    '--delete',
    is_flag=True,
    help='Delete local files that are deleted from remote.'
)
def update(names, creators, include, exclude, ref, delete):
    """Updates files in dataset from a remote Git repo."""
    progress_context = partial(progressbar, label='Updating files')
    update_datasets(
        names=names,
        creators=creators,
        include=include,
        exclude=exclude,
        ref=ref,
        delete=delete,
        progress_context=progress_context
    )
    click.secho('OK', fg='green')
