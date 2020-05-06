# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
r"""Communicator classes for printing output."""

import sys
import threading

from tqdm import tqdm


class CommunicationCallback(object):
    """Base communication class that handles all communication."""

    listeners = []
    lock = threading.Lock()

    @staticmethod
    def subscribe(listener):
        """Add a new listener for communications."""
        with CommunicationCallback.lock:
            if listener not in CommunicationCallback.listeners:
                CommunicationCallback.listeners.append(listener)

    @staticmethod
    def unsubscribe(listener):
        """Remove a communication listener."""
        with CommunicationCallback.lock:
            if listener in CommunicationCallback.listeners:
                CommunicationCallback.listeners.remove(listener)

    def echo(self, msg):
        """Write a message."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.echo(msg)

    def info(self, msg):
        """Write an info message."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.info(msg)

    def warn(self, msg):
        """Write a warning message."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.warn(msg)

    def error(self, msg):
        """Write an error message."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.error(msg)

    def confirm(self, msg, abort=False):
        """Get confirmation for an action."""
        with CommunicationCallback.lock:
            result = True
            for l in CommunicationCallback.listeners:
                result = result and l.confirm(msg, abort)
            return result

    def start_progress(self, name, total, **kwargs):
        """Create a new progress tracker."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.start_progress(name, total, **kwargs)

    def update_progress(self, name, amount):
        """Update a progress tracker."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.update_progress(name, amount)

    def finalize_progress(self, name):
        """End a progress tracker."""
        with CommunicationCallback.lock:
            for l in CommunicationCallback.listeners:
                l.finalize_progress(name)


class StandardOutput(CommunicationCallback):
    """Communication listener that outputs to stdout/stderr."""

    progressbars = {}
    progress_types = ['download']

    def echo(self, msg):
        """Write a message."""
        with CommunicationCallback.lock:
            print(msg)

    def info(self, msg):
        """Write an info message."""
        with CommunicationCallback.lock:
            print(msg)

    def warn(self, msg):
        """Write a warning message."""
        with CommunicationCallback.lock:
            print(msg)

    def error(self, msg):
        """Write an error message."""
        with CommunicationCallback.lock:
            print(msg, file=sys.stderr)

    def confirm(self, msg, abort=False):
        """Get confirmation for an action."""
        return False

    def start_progress(self, name, total, **kwargs):
        """Start a new tqdm progressbar."""
        if name in self.progressbars:
            raise ValueError(
                'Name {} is already a registered progressbar.'.format(name)
            )

        if 'type' not in kwargs:
            kwargs['type'] = 'download'

        if kwargs['type'] not in self.progress_types:
            self.progressbars[name] = None
        elif kwargs['type'] == 'download':
            self.progressbars[name] = tqdm(
                total=total,
                unit='iB',
                unit_scale=True,
                desc=name,
                leave=False,
                bar_format='{desc:.32}: {percentage:3.0f}%|{bar}{r_bar}'
            )

    def update_progress(self, name, amount):
        """Update a progressbar."""
        if name not in self.progressbars or not self.progressbars[name]:
            return

        self.progressbars[name].update(amount)

    def finalize_progress(self, name):
        """End a progressbar."""
        if name not in self.progressbars or not self.progressbars[name]:
            return

        self.progressbars[name].close()
        del self.progressbars[name]


root = CommunicationCallback()
CommunicationCallback.root = root


# utility methods
def subscribe(listener):
    """Subscribe a communication listener."""
    CommunicationCallback.subscribe(listener)


def unsubscribe(listener):
    """Unsubscribe a communication listener."""
    CommunicationCallback.unsubscribe(listener)


def echo(msg):
    """Write a message to all listeners."""
    CommunicationCallback.root.echo(msg)


def info(msg):
    """Write an info message to all listeners."""
    CommunicationCallback.root.info(msg)


def warn(msg):
    """Write a warning message to all listeners."""
    CommunicationCallback.root.warn(msg)


def error(msg):
    """Write an info message to all listeners."""
    CommunicationCallback.root.error(msg)


def confirm(msg, abort=False):
    """Get confirmation for an action from all listeners."""
    CommunicationCallback.root.confirm(msg, abort)


def start_progress(name, total, **kwargs):
    """Start a progress tracker on all listeners."""
    CommunicationCallback.root.start_progress(name, total, **kwargs)


def update_progress(name, amount):
    """Update a progress tracker on all listeners."""
    CommunicationCallback.root.update_progress(name, amount)


def finalize_progress(name):
    """End a progress tracker on all listeners."""
    CommunicationCallback.root.finalize_progress(name)


__all__ = [
    'CommunicationCallback', 'subscribe', 'unsubscribe', 'info', 'warn',
    'error', 'start_progress', 'update_progress', 'finalize_progress'
]
