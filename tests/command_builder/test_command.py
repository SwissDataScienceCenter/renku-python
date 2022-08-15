from typing import Type

import pytest

from renku.command.command_builder import Command
from renku.command.command_builder.database import DatabaseCommand
from renku.command.command_builder.repo import Commit, Isolation, RequireClean


@pytest.mark.parametrize(
    "input,cls,expected",
    [
        (Command(), Command, True),
        (Command(), DatabaseCommand, False),
        (Command().with_git_isolation(), Isolation, True),
        (Command().with_git_isolation(), RequireClean, False),
        (Command().with_git_isolation().with_commit(), Command, True),
        (Command().with_git_isolation().with_commit(), Isolation, True),
        (Command().with_git_isolation().with_commit(), Commit, True),
        (Command().with_git_isolation().with_commit(), DatabaseCommand, False),
        (Command().with_git_isolation().with_commit().with_database(), DatabaseCommand, True),
    ],
)
def test_any_builder_is_instance_of(input: Command, cls: Type, expected: bool):
    assert input.any_builder_is_instance_of(cls) == expected
