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

from renku.core.commands.echo import ERROR


def run_api(addr="0.0.0.0", port=8080, timeout=600, is_debug=False):
    """Run service JSON-RPC API."""
    from gunicorn.app.wsgiapp import run

    svc_num_workers = os.getenv("RENKU_SVC_NUM_WORKERS", "1")
    svc_num_threads = os.getenv("RENKU_SVC_NUM_THREADS", "2")

    loading_opt = "--preload"
    if is_debug:
        loading_opt = "--reload"

    sys.argv = [
        "gunicorn",
        "renku.service.entrypoint:app",
        loading_opt,
        "-b",
        f"{addr}:{port}",
        "--timeout",
        f"{timeout}",
        "--workers",
        svc_num_workers,
        "--worker-class",
        "gthread",
        "--threads",
        svc_num_threads,
        "--log-level",
        "debug",
    ]

    sys.exit(run())


def run_worker(queues):
    """Run service workers."""
    from renku.service.jobs.queues import QUEUES
    from renku.service.worker import start_worker

    if not queues:
        queues = os.getenv("RENKU_SVC_WORKER_QUEUES", "")
        queues = [queue_name.strip() for queue_name in queues.strip().split(",") if queue_name.strip()]

        if not queues:
            queues = QUEUES

    start_worker(queues)


@click.group()
@click.option("-e", "--env", default=None, type=click.Path(exists=True, dir_okay=False), help="Path to the .env file.")
@click.pass_context
def service(ctx, env):
    """Manage service components."""
    try:
        import redis  # noqa: F401
        import rq  # noqa: F401
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env)
    except ImportError:
        # NOTE: Service dependency is missing.

        click.echo(
            ERROR + "Dependency not found! "
            "Please install `pip install renku[service]` to enable service component control."
        )

        ctx.exit(1)


@service.command(name="api")
@click.option(
    "-a",
    "--addr",
    type=str,
    default="0.0.0.0",
    show_default=True,
    help="Address on which API service should listen to. By default uses IPv4.",
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=8080,
    show_default=True,
    help="Port on which API service should listen to. Avoid ports below 1024, for those use reverse-proxies.",
)
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=600,
    show_default=True,
    help="Request silent for more than this many seconds are dropped.",
)
@click.option(
    "-d", "--debug", default=False, is_flag=True, help="Start API in debug mode.",
)
def api_start(addr, port, timeout, debug):
    """Start service JSON-RPC API in active shell session."""
    run_api(addr, port, timeout, debug)


@service.command(name="scheduler")
def scheduler_start():
    """Start service scheduler in active shell session."""
    from renku.service.scheduler import start_scheduler

    start_scheduler()


@service.command(name="worker")
@click.option("-q", "--queue", multiple=True)
def worker_start(queue):
    """Start service worker in active shell session. By default it listens on all queues."""
    run_worker([q.strip() for q in queue if q])
