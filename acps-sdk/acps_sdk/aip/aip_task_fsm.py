"""AIP 任务状态机：ACPs-spec-AIP §4.5「状态转移规则」与「状态转移表」的机器可读形式。

只是规范的照抄和几个判定函数，不改变 SDK 其它部分的行为：``TaskManager``、群组客户端照旧不做校验。
Leader / Partner 想在推进状态、发命令之前核一下合不合规范，可以直接用这里的表和函数::

    from acps_sdk.aip.aip_task_fsm import can_transition, command_is_valid

    if can_transition(task.status.state, TaskState.Completed): ...
    if command_is_valid(TaskCommandType.Continue, partner_state): ...

``None`` 表示「任务还没有」（Start 之前）。
"""

from __future__ import annotations

from typing import Mapping, Optional

from .aip_base_model import TaskCommandType, TaskState

__all__ = [
    "FINAL_STATES",
    "NEXT_STATES",
    "COMMAND_VALID_IN",
    "TaskTransitionError",
    "is_final",
    "can_transition",
    "check_transition",
    "command_is_valid",
]

_S = TaskState

#: 终态：不能再转移到其他状态
FINAL_STATES: frozenset[TaskState] = frozenset(
    {_S.Completed, _S.Canceled, _S.Failed, _S.Rejected}
)

#: 状态转移规则（键为当前状态，None = 任务还没有）
#:   开始后任务状态可以为 Accepted、Rejected（判断后直接进入，不是先 Accepted 再 Rejected）
#:   Accepted → Working / Canceled
#:   Working → AwaitingInput / AwaitingCompletion / Canceled / Failed
#:   AwaitingInput → Working / Canceled（含等待超时取消，表 #11）
#:   AwaitingCompletion → Completed / Working / Canceled（含等待超时完成，表 #15）
NEXT_STATES: Mapping[Optional[TaskState], frozenset[TaskState]] = {
    None: frozenset({_S.Accepted, _S.Rejected}),
    _S.Accepted: frozenset({_S.Working, _S.Canceled}),
    _S.Working: frozenset(
        {_S.AwaitingInput, _S.AwaitingCompletion, _S.Canceled, _S.Failed}
    ),
    _S.AwaitingInput: frozenset({_S.Working, _S.Canceled}),
    _S.AwaitingCompletion: frozenset({_S.Completed, _S.Working, _S.Canceled}),
}

#: 命令在哪些状态下有效（规范：其它状态下收到将被忽略）。Get 不改变状态，任何时候都可以发。
COMMAND_VALID_IN: Mapping[TaskCommandType, frozenset[Optional[TaskState]]] = {
    TaskCommandType.Start: frozenset({None}),
    TaskCommandType.Continue: frozenset({_S.AwaitingInput, _S.AwaitingCompletion}),
    TaskCommandType.Cancel: frozenset(
        {_S.Accepted, _S.Working, _S.AwaitingInput, _S.AwaitingCompletion}
    ),
    TaskCommandType.Complete: frozenset({_S.AwaitingCompletion}),
}


class TaskTransitionError(ValueError):
    """old → new 不是规范允许的转移。"""

    def __init__(self, old: Optional[TaskState], new: TaskState):
        self.old, self.new = old, new
        super().__init__(
            f"AIP task state cannot move from {old.value if old else 'none'} to {new.value}"
        )


def is_final(state: Optional[TaskState]) -> bool:
    return state in FINAL_STATES


def can_transition(
    old: Optional[TaskState], new: TaskState, *, allow_same: bool = True
) -> bool:
    """old → new 合不合规范。

    ``allow_same``：同一个非终态再报一次（同状态下带新数据的状态更新，例如等待输入时补一句话）不算转移，默认放行；
    终态之后什么都不能再报。
    """
    if allow_same and old is not None and old == new and old not in FINAL_STATES:
        return True
    return new in NEXT_STATES.get(old, frozenset())


def check_transition(
    old: Optional[TaskState], new: TaskState, *, allow_same: bool = True
) -> None:
    """不合规范就抛 :class:`TaskTransitionError`。"""
    if not can_transition(old, new, allow_same=allow_same):
        raise TaskTransitionError(old, new)


def command_is_valid(command: TaskCommandType, state: Optional[TaskState]) -> bool:
    """这条命令在对方任务的这个状态下有没有效（无效的按规范会被对方忽略）。"""
    if command == TaskCommandType.Get:
        return True
    return state in COMMAND_VALID_IN.get(command, frozenset())
