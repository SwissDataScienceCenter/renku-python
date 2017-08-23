# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#
"""CLI for the Renga platform."""

import os

import click
from click_plugins import with_plugins
from pkg_resources import iter_entry_points


@with_plugins(iter_entry_points('renga.cli'))
@click.group()
def cli():
    """Base cli."""
    pass


@cli.command()
@click.option('--autosync', is_flag=True)
@click.argument('project_name', nargs=1)
def init(project_name, autosync):
    """Initialize a project."""
    if not autosync:
        raise click.UsageError('You must specify the --autosync option.')

    # 1. create the directory
    try:
        os.mkdir(project_name)
    except FileExistsError:
        raise click.UsageError(
            'Directory {0} already exists'.format(project_name))
