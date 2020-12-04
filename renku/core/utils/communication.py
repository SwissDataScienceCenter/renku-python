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
"""Communicator classes for printing output."""

from functools import wraps
from threading import RLock

from werkzeug.local import Local


class CommunicationCallback:
    """Base communication callback class."""

    lock = RLock()

    def echo(self, msg):
        """Write a message."""

    def info(self, msg):
        """Write an info message."""

    def warn(self, msg):
        """Write a warning message."""

    def error(self, msg):
        """Write an error message."""

    def confirm(self, msg, abort=False, warning=False):
        """Get confirmation for an action."""

    def start_progress(self, name, total, **kwargs):
        """Create a new progress tracker."""

    def update_progress(self, name, amount):
        """Update a progress tracker."""

    def finalize_progress(self, name):
        """End a progress tracker."""

    def has_prompt(self):
        """Return True if communicator provides a direct prompt to users."""

    def prompt(self, msg, type=None, default=None):
        """Show a message prompt."""


class _CommunicationManger(CommunicationCallback):
    """Manages all communication callback objects."""

    def __init__(self):
        super().__init__()
        self._listeners = []

    @property
    def listeners(self):
        """Return subscribed listeners."""
        with CommunicationCallback.lock:
            return self._listeners.copy()

    def subscribe(self, listener):
        """Add a new listener for communications."""
        with CommunicationCallback.lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def unsubscribe(self, listener):
        """Remove a communication listener."""
        with CommunicationCallback.lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def echo(self, msg):
        """Write a message."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.echo(msg)

    def info(self, msg):
        """Write an info message."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.info(msg)

    def warn(self, msg):
        """Write a warning message."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.warn(msg)

    def error(self, msg):
        """Write an error message."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.error(msg)

    def has_prompt(self):
        """Return True if any communicator provides a direct prompt to users."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                if listener.has_prompt():
                    return True

    def confirm(self, msg, abort=False, warning=False):
        """Get confirmation for an action."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                if listener.has_prompt():
                    return listener.confirm(msg, abort, warning)

    def prompt(self, msg, type=None, default=None):
        """Show a message prompt from the first callback that has a prompt."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                if listener.has_prompt():
                    return listener.prompt(msg, type, default)

    def start_progress(self, name, total, **kwargs):
        """Create a new progress tracker."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.start_progress(name, total, **kwargs)

    def update_progress(self, name, amount):
        """Update a progress tracker."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.update_progress(name, amount)

    def finalize_progress(self, name):
        """End a progress tracker."""
        with CommunicationCallback.lock:
            for listener in self._listeners:
                listener.finalize_progress(name)


_thread_local = Local()


def ensure_manager(f):
    """Decorator to add communication manager to local thread storage."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        if getattr(_thread_local, "communication_manager", None) is None:
            _thread_local.communication_manager = _CommunicationManger()

        return f(*args, **kwargs)

    return wrapper


@ensure_manager
def subscribe(listener):
    """Subscribe a communication listener."""
    _thread_local.communication_manager.subscribe(listener)


@ensure_manager
def unsubscribe(listener):
    """Unsubscribe a communication listener."""
    _thread_local.communication_manager.unsubscribe(listener)


@ensure_manager
def echo(msg):
    """Write a message to all listeners."""
    _thread_local.communication_manager.echo(msg)


@ensure_manager
def info(msg):
    """Write an info message to all listeners."""
    _thread_local.communication_manager.info(msg)


@ensure_manager
def warn(msg):
    """Write a warning message to all listeners."""
    _thread_local.communication_manager.warn(msg)


@ensure_manager
def error(msg):
    """Write an info message to all listeners."""
    _thread_local.communication_manager.error(msg)


@ensure_manager
def has_prompt():
    """Return True if communicator provides a direct prompt to users."""
    return True


@ensure_manager
def confirm(msg, abort=False, warning=False):
    """Get confirmation for an action from all listeners."""
    return _thread_local.communication_manager.confirm(msg, abort, warning)


@ensure_manager
def prompt(msg, type=None, default=None):
    """Show a message prompt."""
    return _thread_local.communication_manager.prompt(msg, type, default)


@ensure_manager
def start_progress(name, total, **kwargs):
    """Start a progress tracker on all listeners."""
    _thread_local.communication_manager.start_progress(name, total, **kwargs)


@ensure_manager
def update_progress(name, amount):
    """Update a progress tracker on all listeners."""
    _thread_local.communication_manager.update_progress(name, amount)


@ensure_manager
def finalize_progress(name):
    """End a progress tracker on all listeners."""
    _thread_local.communication_manager.finalize_progress(name)


@ensure_manager
def get_listeners():
    """Return a list of subscribed listeners."""
    return _thread_local.communication_manager.listeners


__all__ = [
    "CommunicationCallback",
    "subscribe",
    "unsubscribe",
    "info",
    "warn",
    "error",
    "has_prompt",
    "confirm",
    "prompt",
    "start_progress",
    "update_progress",
    "finalize_progress",
]
