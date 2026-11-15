"""aip_task_fsm 与 ACPs-spec-AIP §4.5 的状态转移表逐行对照。"""
from __future__ import annotations

import itertools

import pytest

from acps_sdk.aip.aip_base_model import TaskCommandType, TaskState
from acps_sdk.aip.aip_task_fsm import (
    FINAL_STATES,
    TaskTransitionError,
    can_transition,
    check_transition,
    command_is_valid,
    is_final,
)

S = TaskState

#: 规范「状态转移表」#1–#15（#16–#19 是终态）
SPEC_ROWS = [
    (None, S.Accepted), (None, S.Rejected),
    (S.Accepted, S.Working), (S.Accepted, S.Canceled),
    (S.Working, S.AwaitingCompletion), (S.Working, S.AwaitingInput), (S.Working, S.Failed), (S.Working, S.Canceled),
    (S.AwaitingInput, S.Working), (S.AwaitingInput, S.Canceled),
    (S.AwaitingCompletion, S.Completed), (S.AwaitingCompletion, S.Working), (S.AwaitingCompletion, S.Canceled),
]


def test_spec_rows_are_allowed_and_nothing_else() -> None:
    states = [None, *TaskState]
    for old, new in itertools.product(states, list(TaskState)):
        expected = (old, new) in SPEC_ROWS
        assert can_transition(old, new, allow_same=False) is expected, (old, new)


def test_same_state_update_is_allowed_only_before_final() -> None:
    assert can_transition(S.AwaitingInput, S.AwaitingInput)
    assert can_transition(S.Working, S.Working)
    for final in FINAL_STATES:
        assert not can_transition(final, final)
        assert is_final(final)
    assert can_transition(None, S.Accepted)
    assert not can_transition(None, S.Working)


def test_check_transition_raises() -> None:
    with pytest.raises(TaskTransitionError):
        check_transition(S.AwaitingInput, S.Completed)
    check_transition(S.AwaitingCompletion, S.Completed)


@pytest.mark.parametrize(
    "command, valid",
    [
        (TaskCommandType.Start, {None}),
        (TaskCommandType.Continue, {S.AwaitingInput, S.AwaitingCompletion}),
        (TaskCommandType.Cancel, {S.Accepted, S.Working, S.AwaitingInput, S.AwaitingCompletion}),
        (TaskCommandType.Complete, {S.AwaitingCompletion}),
        (TaskCommandType.Get, {None, *TaskState}),
    ],
)
def test_command_validity_matches_spec(command, valid) -> None:
    for state in [None, *TaskState]:
        assert command_is_valid(command, state) is (state in valid), (command, state)
