# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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


class CommunicationCallback:
    """Base communication callback class."""

    lock = RLock()

    def echo(self, msg, end="\n"):
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

    def prompt(self, msg, type=None, default=None, **kwargs):
        """Show a message prompt."""


def lock_communication(method):
    """Ensure communicator is locked."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """Implementation of method wrapper."""
        with CommunicationCallback.lock:
            return method(self, *args, **kwargs)

    return wrapper


def ensure_communication(method):
    """Ensure communicator is enabled."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """Implementation of method wrapper."""
        if self._enabled:
            return method(self, *args, **kwargs)

    return wrapper


class _CommunicationManger(CommunicationCallback):
    """Manages all communication callback objects."""

    def __init__(self):
        super().__init__()
        self._listeners = []
        self._enabled = True

    @property
    @lock_communication
    def listeners(self):
        """Return subscribed listeners."""
        return self._listeners.copy()

    @lock_communication
    def subscribe(self, listener):
        """Add a new listener for communications."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    @lock_communication
    def unsubscribe(self, listener):
        """Remove a communication listener."""
        if listener in self._listeners:
            self._listeners.remove(listener)

    @lock_communication
    @ensure_communication
    def echo(self, msg, end="\n"):
        """Write a message."""
        for listener in self._listeners:
            listener.echo(msg, end=end)

    @lock_communication
    @ensure_communication
    def info(self, msg):
        """Write an info message."""
        for listener in self._listeners:
            listener.info(msg)

    @lock_communication
    @ensure_communication
    def warn(self, msg):
        """Write a warning message."""
        for listener in self._listeners:
            listener.warn(msg)

    @lock_communication
    @ensure_communication
    def error(self, msg):
        """Write an error message."""
        for listener in self._listeners:
            listener.error(msg)

    @lock_communication
    def has_prompt(self):
        """Return True if any communicator provides a direct prompt to users."""
        for listener in self._listeners:
            if listener.has_prompt():
                return True

    @lock_communication
    def confirm(self, msg, abort=False, warning=False):
        """Get confirmation for an action."""
        for listener in self._listeners:
            if listener.has_prompt():
                return listener.confirm(msg, abort, warning)

    @lock_communication
    def prompt(self, msg, type=None, default=None, **kwargs):
        """Show a message prompt from the first callback that has a prompt."""
        for listener in self._listeners:
            if listener.has_prompt():
                return listener.prompt(msg, type, default, **kwargs)

    @lock_communication
    @ensure_communication
    def start_progress(self, name, total, **kwargs):
        """Create a new progress tracker."""
        for listener in self._listeners:
            listener.start_progress(name, total, **kwargs)

    @lock_communication
    @ensure_communication
    def update_progress(self, name, amount):
        """Update a progress tracker."""
        for listener in self._listeners:
            listener.update_progress(name, amount)

    @lock_communication
    @ensure_communication
    def finalize_progress(self, name):
        """End a progress tracker."""
        for listener in self._listeners:
            listener.finalize_progress(name)

    @lock_communication
    def disable(self):
        """Disable all outputs; by default everything is enabled."""
        self._enabled = False

    @lock_communication
    def enable(self):
        """Enable all outputs."""
        self._enabled = True


_thread_local = None


def ensure_manager(f):
    """Decorator to add communication manager to local thread storage."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        global _thread_local

        if _thread_local is None:
            from werkzeug.local import Local

            _thread_local = Local()

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
def echo(msg, end="\n"):
    """Write a message to all listeners."""
    _thread_local.communication_manager.echo(msg, end=end)


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
def prompt(msg, type=None, default=None, **kwargs):
    """Show a message prompt."""
    return _thread_local.communication_manager.prompt(msg, type, default, **kwargs)


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


@ensure_manager
def disable():
    """Disable all outputs; by default everything is enabled."""
    return _thread_local.communication_manager.disable()


@ensure_manager
def enable():
    """Enable all outputs."""
    return _thread_local.communication_manager.enable()


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
    "disable",
    "enable",
]
