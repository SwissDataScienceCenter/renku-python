import pytest

from renku.ui.cli import cli


@pytest.fixture()
def create_s3_dataset(runner):
    def _create_s3_dataset(name, uri):
        res = runner.invoke(cli, ["dataset", "create", name, "--storage", uri])
        return res

    yield _create_s3_dataset
