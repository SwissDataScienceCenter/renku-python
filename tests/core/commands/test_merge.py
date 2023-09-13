#
# Copyright 2017-2023- Swiss Data Science Center (SDSC)
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
"""Renku mergetool command tests."""

from uuid import uuid4

from BTrees.OOBTree import BTree

from renku.domain_model.dataset import Dataset
from renku.domain_model.project import Project, ProjectTemplateMetadata
from renku.domain_model.provenance.agent import Person
from renku.domain_model.workflow.plan import Plan
from renku.infrastructure.database import Index
from renku.infrastructure.git_merger import GitMerger


def test_merge_dataset_tree_non_conflict_dataset():
    """Test merging dataset tree with non-conflicting changes."""
    dataset1 = Dataset(id=Dataset.generate_id(uuid4().hex), name="dataset1")
    dataset2 = Dataset(id=Dataset.generate_id(uuid4().hex), name="dataset2")

    local = BTree({"dataset1": dataset1})
    remote = BTree({"dataset2": dataset2})

    merged = GitMerger().merge_btrees(local, remote)
    assert "dataset1" in merged
    assert "dataset2" in merged


def test_merge_dataset_tree_local_modified_dataset(mocker):
    """Test merging dataset tree with local modifications."""
    dataset = Dataset(id=Dataset.generate_id(uuid4().hex), slug="dataset", name="First")
    modified_dataset = dataset.copy()
    modified_dataset.update_metadata(name="Modified name")
    modified_dataset.derive_from(dataset, creator=None)

    local = BTree({"dataset": modified_dataset})
    remote = BTree({"dataset": dataset})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: dataset if id == dataset.id else modified_dataset
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    merged = merger.merge_btrees(local, remote)
    assert "dataset" in merged.keys()
    assert "Modified name" == merged["dataset"].name


def test_merge_dataset_tree_remote_modified_dataset(mocker):
    """Test merging dataset tree with remote modifications."""
    dataset = Dataset(id=Dataset.generate_id(uuid4().hex), slug="dataset", name="First")
    modified_dataset = dataset.copy()
    modified_dataset.update_metadata(name="Modified name")
    modified_dataset.derive_from(dataset, creator=None)

    local = BTree({"dataset": dataset})
    remote = BTree({"dataset": modified_dataset})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: dataset if id == dataset.id else modified_dataset
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    merged = merger.merge_btrees(local, remote)
    assert "dataset" in merged.keys()
    assert "Modified name" == merged["dataset"].name


def test_merge_dataset_tree_both_modified_dataset(mocker):
    """Test merging dataset tree with remote and local modifications."""
    dataset = Dataset(id=Dataset.generate_id(uuid4().hex), slug="dataset", name="First")
    modified_local_dataset = dataset.copy()
    modified_local_dataset.update_metadata(name="Modified local name")
    modified_local_dataset.derive_from(dataset, creator=None)
    modified_remote_dataset = dataset.copy()
    modified_remote_dataset.update_metadata(name="Modified remote name")
    modified_remote_dataset.derive_from(dataset, creator=None)

    unrelated_dataset = Dataset(id=Dataset.generate_id(uuid4().hex), slug="unrelated_dataset", name="unrelated")

    all_datasets = [dataset, modified_local_dataset, modified_remote_dataset, unrelated_dataset]

    local = BTree({"dataset": modified_local_dataset, "unrelated_dataset": unrelated_dataset})
    remote = BTree({"dataset": modified_remote_dataset, "unrelated_dataset": unrelated_dataset})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: next(d for d in all_datasets if d.id == id)
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="l"))
    merged = merger.merge_btrees(local, remote)
    assert "dataset" in merged.keys()
    assert "Modified local name" == merged["dataset"].name
    assert "unrelated_dataset" in merged.keys()
    assert "unrelated" == merged["unrelated_dataset"].name

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="r"))
    merged = merger.merge_btrees(local, remote)
    assert "dataset" in merged.keys()
    assert "Modified remote name" == merged["dataset"].name
    assert "unrelated_dataset" in merged.keys()
    assert "unrelated" == merged["unrelated_dataset"].name


def test_merge_plan_tree_non_conflict_plan():
    """Test merging plan tree with non-conflicting changes."""
    plan1 = Plan(id=Plan.generate_id(), name="plan1", command="echo")
    plan2 = Plan(id=Plan.generate_id(), name="plan2", command="echo")

    local = BTree({"plan1": plan1})
    remote = BTree({"plan2": plan2})

    merged = GitMerger().merge_btrees(local, remote)
    assert "plan1" in merged
    assert "plan2" in merged


def test_merge_plan_tree_local_modified_plan(mocker):
    """Test merging plan tree with local modifications."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="echo")
    modified_plan = plan.derive()
    modified_plan.keywords = ["modified"]

    local = BTree({"plan": modified_plan})
    remote = BTree({"plan": plan})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: plan if id == plan.id else modified_plan
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_plan.keywords == merged["plan"].keywords


def test_merge_plan_tree_remote_modified_plan(mocker):
    """Test merging plan tree with remote modifications."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="echo")
    modified_plan = plan.derive()
    modified_plan.keywords = ["modified"]

    local = BTree({"plan": plan})
    remote = BTree({"plan": modified_plan})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: plan if id == plan.id else modified_plan
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_plan.keywords == merged["plan"].keywords


def test_merge_plan_tree_both_modified_plan(mocker):
    """Test merging plan tree with local and remote modifications."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="echo")
    modified_local_plan = plan.derive()
    modified_local_plan.keywords = ["modified"]
    modified_remote_plan = plan.derive()
    modified_remote_plan.keywords = ["also_modified"]

    unrelated_plan = Plan(id=Plan.generate_id(), name="unrelated_plan", command="echo")
    unrelated_plan.keywords = ["unrelated"]

    all_plans = [plan, modified_local_plan, modified_remote_plan, unrelated_plan]

    local = BTree({"plan": modified_local_plan, "unrelated_plan": unrelated_plan})
    remote = BTree({"plan": modified_remote_plan, "unrelated_plan": unrelated_plan})

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: next(d for d in all_plans if d.id == id)
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="l"))
    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_local_plan.keywords == merged["plan"].keywords
    assert "unrelated_plan" in merged.keys()
    assert unrelated_plan.keywords == merged["unrelated_plan"].keywords

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="r"))
    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_remote_plan.keywords == merged["plan"].keywords
    assert "unrelated_plan" in merged.keys()
    assert unrelated_plan.keywords == merged["unrelated_plan"].keywords


def test_merge_plan_index_both_modified_plan(mocker):
    """Test merging plan index with local and remote modifications."""
    plan = Plan(id=Plan.generate_id(), name="plan", command="echo")
    modified_local_plan = plan.derive()
    modified_local_plan.keywords = ["modified"]
    modified_remote_plan = plan.derive()
    modified_remote_plan.keywords = ["also_modified"]

    unrelated_plan = Plan(id=Plan.generate_id(), name="unrelated_plan", command="echo")
    unrelated_plan.keywords = ["unrelated"]

    all_plans = [plan, modified_local_plan, modified_remote_plan, unrelated_plan]

    local = Index(name="plans", object_type=Plan, attribute="name")
    {"plan": modified_local_plan, "unrelated_plan": unrelated_plan}
    local.add(unrelated_plan)
    local.add(modified_local_plan)
    remote = Index(name="plans", object_type=Plan, attribute="name")
    remote.add(unrelated_plan)
    remote.add(modified_remote_plan)

    database = mocker.MagicMock()
    database.get_by_id.side_effect = lambda id: next(d for d in all_plans if d.id == id)
    merger = GitMerger()
    merger.local_database = database
    merger.remote_database = database

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="l"))
    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_local_plan.keywords == merged["plan"].keywords
    assert "unrelated_plan" in merged.keys()
    assert unrelated_plan.keywords == merged["unrelated_plan"].keywords

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="r"))
    merged = merger.merge_btrees(local, remote)
    assert "plan" in merged.keys()
    assert modified_remote_plan.keywords == merged["plan"].keywords
    assert "unrelated_plan" in merged.keys()
    assert unrelated_plan.keywords == merged["unrelated_plan"].keywords


def test_merge_project_local_changes():
    """Test merging project with local changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", description="My project"
    )
    local_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", description="My modified project"
    )

    result = GitMerger().merge_projects(local_project, base_project, base_project)

    assert result == local_project


def test_merge_project_remote_changes():
    """Test merging project with remote changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", description="My project"
    )
    remote_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", description="My modified project"
    )

    result = GitMerger().merge_projects(base_project, remote_project, base_project)

    assert result == remote_project


def test_merge_project_both_changed():
    """Test merging project with local and remote changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", keywords=["great"]
    )
    local_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        keywords=["datascience"],
        version="8",
    )
    remote_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"), name="my-project", keywords=["awesome"], version="9"
    )

    result = GitMerger().merge_projects(local_project, remote_project, base_project)

    assert set(result.keywords) == {"datascience", "awesome"}
    assert "9" == result.version
    assert result.agent_version


def test_merge_project_both_template_changed(mocker):
    """Test merging project with local and remote template changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal",
            template_source="renku",
            template_ref="master",
            template_version="abcdef",
        ),
    )
    local_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal1",
            template_source="renku1",
            template_ref="master1",
            template_version="12345",
        ),
    )
    remote_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal2",
            template_source="renku2",
            template_ref="master2",
            template_version="78910",
        ),
    )

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="l"))

    result = GitMerger().merge_projects(local_project, remote_project, base_project)

    assert result.template_metadata == local_project.template_metadata

    mocker.patch("renku.infrastructure.git_merger.communication.prompt", mocker.MagicMock(return_value="r"))

    result = GitMerger().merge_projects(local_project, remote_project, base_project)

    assert result.template_metadata == remote_project.template_metadata


def test_merge_project_local_template_changed():
    """Test merging project with local template changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal",
            template_source="renku",
            template_ref="master",
            template_version="abcdef",
        ),
    )
    local_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal1",
            template_source="renku1",
            template_ref="master1",
            template_version="12345",
        ),
    )
    remote_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal",
            template_source="renku",
            template_ref="master",
            template_version="abcdef",
        ),
    )

    result = GitMerger().merge_projects(local_project, remote_project, base_project)

    assert result.template_metadata == local_project.template_metadata


def test_merge_project_remote_template_changed():
    """Test merging project with remote template changes."""
    base_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal",
            template_source="renku",
            template_ref="master",
            template_version="abcdef",
        ),
    )
    local_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal",
            template_source="renku",
            template_ref="master",
            template_version="abcdef",
        ),
    )
    remote_project = Project(
        creator=Person.from_string("John Doe <jd@example.com>"),
        name="my-project",
        template_metadata=ProjectTemplateMetadata(
            template_id="python-minimal1",
            template_source="renku1",
            template_ref="master1",
            template_version="12345",
        ),
    )

    result = GitMerger().merge_projects(local_project, remote_project, base_project)

    assert result.template_metadata == remote_project.template_metadata
