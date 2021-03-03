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
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import click
import psutil

from renku.core.commands.echo import ERROR
from renku.core.models.tabulate import tabulate
from renku.core.utils.contexts import chdir

RENKU_DAEMON_LOG_FILE = "renku.log"
RENKU_DAEMON_ERR_FILE = "renku.err"


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


def check_cmdline(cmdline, include=None):
    """Check `cmdline` command of a process."""
    include = include or []
    service_components = include + ["api", "scheduler", "worker"]

    for cmd in service_components:
        if cmd in cmdline:
            return True

    return False


def list_renku_processes(include=None):
    """List renku processes."""
    include = include or []
    processes = [psutil.Process(pid) for pid in psutil.pids()]

    renku_processes = []
    for proc in processes:
        try:
            if check_cmdline(proc.cmdline(), include) and (
                proc.name() == "renku" or check_cmdline(proc.cmdline(), ["renku"])
            ):
                renku_processes.append(proc)
        except psutil.AccessDenied:
            pass

    renku_proc_info = sorted(
        [
            {
                "create_time": datetime.fromtimestamp(proc.create_time()).strftime("%d.%b %H:%M"),
                "pid": proc.pid,
                "cmdline": f"renku {' '.join(proc.cmdline()[2:])}",
                "status": proc.status(),
                "mem_perct": proc.memory_percent(),
                "cpu_perct": proc.cpu_percent(),
                "num_threads": proc.num_threads(),
                "num_fds": proc.num_fds(),
            }
            for proc in renku_processes
        ],
        key=lambda k: k["cmdline"],
    )

    return renku_proc_info


def read_logs(log_file, follow=True, output_all=False):
    """Read logs file. Supports following logs in realtime."""
    if follow and not output_all:
        log_file.seek(0, os.SEEK_END)

    while True:
        line = log_file.readline()
        if not line and follow:
            time.sleep(0.1)
            continue

        if not line and not follow:
            return

        yield line


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


@service.command(name="ps")
@click.pass_context
def ps(ctx):
    """Check status of running services."""
    processes = list_renku_processes()
    headers = [{k.upper(): v for k, v in rec.items()} for rec in processes]

    output = tabulate(processes, headers=headers,)

    if not processes:
        click.echo("Renku service components are down.")
        ctx.exit()

    click.echo(output)


@service.command(name="up")
@click.option("-d", "--daemon", is_flag=True, default=False, help="Starts all processes in daemon mode.")
@click.option("-rd", "--runtime-dir", default=".", help="Directory for runtime metadata in daemon mode.")
@click.pass_context
def all_start(ctx, daemon, runtime_dir):
    """Start all service components in daemon mode."""
    from circus import get_arbiter

    services = [
        {
            "name": "RenkuCoreService",
            "cmd": "renku",
            "args": ["service", "api"],
            "numprocesses": 1,
            "env": os.environ.copy(),
            "shell": True,
        },
        {
            "name": "RenkuCoreScheduler",
            "cmd": "renku",
            "args": ["service", "scheduler"],
            "numprocesses": 1,
            "env": os.environ.copy(),
            "shell": True,
        },
        {
            "name": "RenkuCoreWorker",
            "cmd": "renku",
            "args": ["service", "worker"],
            "numprocesses": 1,
            "env": os.environ.copy(),
            "shell": True,
        },
    ]

    def launch_arbiter(arbiter):
        """Helper for launching arbiter process."""
        with chdir(runtime_dir):
            try:
                arbiter.start()
            finally:
                arbiter.stop()

    if not daemon:
        launch_arbiter(get_arbiter(services))
        ctx.exit()

    # NOTE: If we are running in daemon mode, the runtime directory is generated is OS /tmp directory.
    # Since in this case daemon is long running process we don't want to pollute user space.
    if not runtime_dir or runtime_dir == ".":
        runtime_dir = tempfile.mkdtemp()

    os.environ["CACHE_DIR"] = runtime_dir
    click.echo(f"Using runtime directory: {runtime_dir}")

    log_stdout = Path(runtime_dir) / RENKU_DAEMON_LOG_FILE
    log_stderr = Path(runtime_dir) / RENKU_DAEMON_ERR_FILE

    subprocess.Popen(
        ["renku", "service", "up", "--runtime-dir", runtime_dir],
        stdout=log_stdout.open(mode="w"),
        stderr=log_stderr.open(mode="w"),
        start_new_session=True,
    )

    click.secho("OK", fg="green")


@service.command(name="down")
def all_stop():
    """Stop all service components."""
    # NOTE: We include `renku service up` because that process contains the arbiter and watcher.
    processes = list_renku_processes(["up"])

    for proc in processes:
        click.echo(f"Shutting down [{proc['pid']}] `{proc['cmdline']}`")
        try:
            os.kill(proc["pid"], signal.SIGKILL)
        except ProcessLookupError:
            click.echo(f"Process [{proc['pid']}] `{proc['cmdline']}` not found - skipping")
            continue

    if processes:
        click.secho("OK", fg="green")
    else:
        click.echo("Nothing to shut down.")


@service.command(name="restart")
def all_restart():
    """Restart all running service components."""
    processes = list_renku_processes()

    for proc in processes:
        click.echo(f"Restarting `{proc['cmdline']}`")
        os.kill(proc["pid"], signal.SIGKILL)

    if processes:
        click.secho("OK", fg="green")
    else:
        click.echo("Nothing to restart.")


@service.command(name="logs")
@click.option("-f", "--follow", is_flag=True, default=False, help="Follows logs of damonized service components.")
@click.option(
    "-a", "--output-all", is_flag=True, default=False, help="Outputs ALL logs of damonized service components."
)
@click.option("-e", "--errors", is_flag=True, default=False, help="Outputs all errors of damonized service components.")
@click.pass_context
def all_logs(ctx, follow, output_all, errors):
    """Check logs of all running daemonized service components."""
    processes = list_renku_processes(["up"])

    if not processes:
        click.echo("Daemonized component processes are not running.\nStart them with `renku service up --daemon`")
        ctx.exit()

    for proc in processes:
        if "cmdline" in proc and "up" in proc["cmdline"]:
            runtime_dir = Path(proc["cmdline"].split("--runtime-dir")[-1].strip())

            stream = runtime_dir / RENKU_DAEMON_LOG_FILE
            if errors:
                stream = runtime_dir / RENKU_DAEMON_ERR_FILE

            for line in read_logs(stream.open(mode="r"), follow=follow, output_all=output_all):
                click.echo(line)
