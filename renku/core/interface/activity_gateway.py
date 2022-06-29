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
"""Renku activity gateway interface."""

from abc import ABC
from pathlib import Path
from typing import List, Optional, Set, Tuple, Union

from renku.domain_model.provenance.activity import Activity, ActivityCollection


class IActivityGateway(ABC):
    """Interface for the ActivityGateway."""

    def get_by_id(self, id: str) -> Optional[Activity]:
        """Get an activity by id."""
        raise NotImplementedError

    def get_all_usage_paths(self) -> List[str]:
        """Return all usage paths."""
        raise NotImplementedError

    def get_all_generation_paths(self) -> List[str]:
        """Return all generation paths."""
        raise NotImplementedError

    def get_activities_by_usage(self, path: Union[Path, str], checksum: Optional[str] = None) -> List[Activity]:
        """Return the list of all activities that use a path."""
        raise NotImplementedError

    def get_activities_by_generation(self, path: Union[Path, str], checksum: Optional[str] = None) -> List[Activity]:
        """Return the list of all activities that generate a path."""
        raise NotImplementedError

    def get_downstream_activities(self, activity: Activity, max_depth=None) -> Set[Activity]:
        """Get downstream activities that depend on this activity."""
        raise NotImplementedError

    def get_upstream_activities(self, activity: Activity, max_depth=None) -> Set[Activity]:
        """Get upstream activities that this activity depends on."""
        raise NotImplementedError

    def get_downstream_activity_chains(self, activity: Activity) -> List[Tuple[Activity, ...]]:
        """Get a list of tuples of all downstream paths of this activity."""
        raise NotImplementedError

    def get_upstream_activity_chains(self, activity: Activity) -> List[Tuple[Activity, ...]]:
        """Get a list of tuples of all upstream paths of this activity."""
        raise NotImplementedError

    def get_all_activities(self, include_deleted: bool = False) -> List[Activity]:
        """Get all activities in the project."""
        raise NotImplementedError

    def add(self, activity: Activity) -> None:
        """Add an ``Activity`` to storage."""
        raise NotImplementedError

    def add_activity_collection(self, activity_collection: ActivityCollection):
        """Add an ``ActivityCollection`` to storage."""
        raise NotImplementedError

    def get_all_activity_collections(self) -> List[ActivityCollection]:
        """Get all activity collections in the project."""
        raise NotImplementedError

    def remove(self, activity: Activity, keep_reference: bool = True, force: bool = False):
        """Remove an activity from the storage.

        Args:
            activity(Activity): The activity to be removed.
            keep_reference(bool): Whether to keep the activity in the ``activities`` index or not.
            force(bool): Force-delete the activity even if it has downstream activities.
        """
        raise NotImplementedError
