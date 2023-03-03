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
"""test KG against SHACL shape."""

from pathlib import Path

import pyld
import pytest

from renku.command.schema.dataset import dump_dataset_as_jsonld
from renku.core.util.shacl import validate_graph
from renku.domain_model.project_context import project_context
from renku.ui.cli import cli
from tests.utils import load_dataset


@pytest.mark.skip(reason="FIXME correct this when implementing renku graph export")
def test_dataset_shacl(tmpdir, runner, project):
    """Test dataset metadata structure."""
    force_dataset_path = Path(__file__).parent.parent.parent / "data" / "force_dataset_shacl.json"

    force_datasetfile_path = Path(__file__).parent.parent.parent / "data" / "force_datasetfile_shacl.json"

    force_datasettag_path = Path(__file__).parent.parent.parent / "data" / "force_datasettag_shacl.json"

    runner.invoke(cli, ["dataset", "create", "dataset"])

    paths = []
    for i in range(3):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    runner.invoke(cli, ["dataset", "add", "--copy", "dataset"] + paths, catch_exceptions=False)

    runner.invoke(cli, ["dataset", "tag", "dataset", "1.0"], catch_exceptions=False)

    dataset = load_dataset("dataset")
    g = dump_dataset_as_jsonld(dataset)
    rdf = pyld.jsonld.to_rdf(g, options={"format": "application/n-quads", "produceGeneralizedRdf": True})

    r, _, t = validate_graph(rdf, shacl_path=str(force_dataset_path))
    assert r is True, t

    r, _, t = validate_graph(rdf, shacl_path=str(force_datasetfile_path))
    assert r is True, t

    r, _, t = validate_graph(rdf, shacl_path=str(force_datasettag_path))
    assert r is True, t

    r, _, t = validate_graph(rdf)
    assert r is True, t


def test_project_shacl(project, with_injection):
    """Test project metadata structure."""
    from renku.command.schema.project import ProjectSchema
    from renku.domain_model.provenance.agent import Person

    path = Path(__file__).parent.parent.parent / "data" / "force_project_shacl.json"

    with with_injection():
        project = project_context.project
        project.creator = Person(email="johndoe@example.com", name="Johnny Doe")

        g = ProjectSchema().dump(project)

    g["@id"] = "https://localhost/" + g["@id"]
    rdf = pyld.jsonld.to_rdf(g, options={"format": "application/n-quads", "produceGeneralizedRdf": False})
    r, _, t = validate_graph(rdf, shacl_path=str(path))
    assert r is True, t

    r, _, t = validate_graph(rdf)
    assert r is True, t
