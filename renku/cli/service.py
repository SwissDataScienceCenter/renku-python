# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Commands to launch service components."""
import os
import sys

import click
from gunicorn.app.wsgiapp import run

from renku.core.commands.echo import ERROR
from renku.service.jobs.queues import QUEUES
from renku.service.scheduler import start_scheduler
from renku.service.worker import start_worker


def run_api():
    """Run service JSON-RPC API."""
    svc_num_workers = os.getenv("RENKU_SVC_NUM_WORKERS", "1")
    svc_num_threads = os.getenv("$RENKU_SVC_NUM_THREADS", "2")

    sys.argv = [
        "gunicorn",
        "renku.service.entrypoint:app",
        "-b" "0.0.0.0:8080",
        "--timeout",
        "600",
        "--workers",
        svc_num_workers,
        "--worker-class",
        "gthread",
        "--threads",
        svc_num_threads,
    ]

    sys.exit(run())


def run_worker(queues):
    """Run service workers."""
    if not queues:
        queues = os.getenv("RENKU_SVC_WORKER_QUEUES", "")
        queues = [queue_name.strip() for queue_name in queues.strip().split(",")]

        if not queues:
            queues = QUEUES

    start_worker(queues)


@click.group()
@click.option("-e", "--env", default=None, type=click.Path(exists=True, dir_okay=False), help="Path to the .env file.")
def service(env):
    """Manage service components."""
    try:
        import redis  # noqa: F401
        from dotenv import load_dotenv
    except ImportError:
        # NOTE: Service dependency is missing.

        click.echo(
            ERROR + "Dependency not found! "
            "Please install `pip install renku[service]` to enable service component control."
        )

    load_dotenv(dotenv_path=env)


@service.command(name="api")
def api_start():
    """Start service JSON-RPC API in active shell session."""
    run_api()


@service.command(name="scheduler")
def scheduler_start():
    """Start service scheduler in active shell session."""
    start_scheduler()


@service.command(name="worker")
@click.option("-q", "--queue", multiple=True)
def worker_start(queue):
    """Start service worker in active shell session. By default it listens on all queues."""
    run_worker(list(queue))
