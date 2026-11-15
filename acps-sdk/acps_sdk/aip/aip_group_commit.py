"""群组的「全体确认才生效」：Leader 侧的两阶段提交协调器。

只用 AIP 已有的命令和任务状态，不新增消息类型、不新增状态，参与方不需要知道有这个协调器——
任何照规范实现的 Partner 都能参加：

============  ===========================================================================
阶段           AIP 上是什么
============  ===========================================================================
提案           Leader 在群里点名每个参与方，发 Start（开新任务）或 Continue（接着已有任务），带上提案
表态           参与方回 AwaitingCompletion = 同意（交了产出物，等 Leader 裁决）；
               AwaitingInput = 要改 / 要补信息；Rejected / Failed / Canceled = 不接
提交           全部同意 → 逐个 Complete
撤回（补偿）    有一家不成（含超时、被调用方否决）→ 对还没终结的逐个 Cancel
============  ===========================================================================

为什么这样是对的：规范里 AwaitingCompletion 只能由 Leader 的 Complete / Continue / Cancel 离开
（唯一的例外是表 #15「等待超时，任务成功结束」），所以全部到了 AwaitingCompletion 以后提交不会被参与方
自己打断；而 Continue 只在 AwaitingInput / AwaitingCompletion 下有效（其它状态下会被忽略），所以发提案前
先等对方说完上一轮。每一家这一票只认**发出提案之后**到的、表了态的那条回执；票在裁决前定格，之后发取消
引起的 canceled 回执不会回头改写结果。

用法::

    commit = GroupCommit(leader, session_id)
    outcome = await commit.run(
        [CommitParticipant(partner_aic=a, task_id=t, text=proposal_text) for a, t in members],
        timeout=60,
    )
    if outcome.committed: ...
    for p in outcome.participants:
        print(p.partner_aic, p.vote, p.detail, p.decision)

要在收齐和裁决之间插一步（让用户确认、核对产出物），分开调 :meth:`GroupCommit.prepare` 和
:meth:`GroupCommit.commit` / :meth:`GroupCommit.abort`；要改了再提，用同一批任务再 ``prepare`` 一次
（Continue 在 AwaitingInput / AwaitingCompletion 下都有效）。

注意：参与方可能在 AwaitingCompletion 等待超时后自己转成 Completed（规范表 #15），那之后就撤不回了。
``timeout`` 应短于参与方的这个等待时限；撤回时碰到这种情况，会记在 :attr:`CommitOutcome.compensation_failed`。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .aip_base_model import DataItem, TaskCommandType, TaskResult, TaskState
from .aip_task_fsm import command_is_valid, is_final

if TYPE_CHECKING:  # pragma: no cover
    from .aip_group_leader import GroupLeader, GroupLeaderSession

logger = logging.getLogger("acps_sdk.aip.group_commit")

__all__ = [
    "CommitParticipant",
    "CommitOutcome",
    "GroupCommit",
    "ParticipantOutcome",
    "Vote",
]

_Key = Tuple[str, str]

#: 表了态的状态：收到这几种之一，这一家这一票就定了
_DECISIVE = frozenset(
    {
        TaskState.AwaitingCompletion,
        TaskState.AwaitingInput,
        TaskState.Rejected,
        TaskState.Failed,
        TaskState.Canceled,
        TaskState.Completed,
    }
)


@dataclass
class CommitParticipant:
    """一个参与方的一个任务。同一个 Partner 可以以不同任务号出现多次（例如一个平台下的几家店）。"""

    partner_aic: str
    task_id: str
    #: 给它的提案正文（TextDataItem）。不懂结构化数据的参与方只读这句
    text: Optional[str] = None
    #: 附带的数据项（例如承载提案的 StructuredDataItem）
    data_items: Optional[List[DataItem]] = None
    #: True：用 Start 开一个新任务（task_id 须是新的）；False：用 Continue 接着已有的任务
    start: bool = False

    @property
    def key(self) -> _Key:
        return (self.partner_aic, self.task_id)


class Vote(str, Enum):
    """一个参与方这一轮的结果。"""

    ACCEPTED = "accepted"  #: 回了 AwaitingCompletion，且没被否决
    NEEDS_INPUT = "needs-input"  #: 回了 AwaitingInput（要改、要补信息）
    REJECTED = "rejected"
    FAILED = "failed"
    CANCELED = "canceled"
    VETOED = "vetoed"  #: 回了 AwaitingCompletion，但调用方的 vet 否决了
    TIMEOUT = "timeout"  #: 到时没有表态（还在 Accepted / Working，或一条都没回）
    NOT_SENT = "not-sent"  #: 对方的任务当时收不了这条命令（已终结，或等不到可以 Continue 的状态），没有发
    SEND_FAILED = "send-failed"  #: 发提案时出错
    UNEXPECTED = "unexpected"  #: 回了此时不该出现的状态（例如自己报 Completed）


_VOTE_OF_STATE = {
    TaskState.AwaitingCompletion: Vote.ACCEPTED,
    TaskState.AwaitingInput: Vote.NEEDS_INPUT,
    TaskState.Rejected: Vote.REJECTED,
    TaskState.Failed: Vote.FAILED,
    TaskState.Canceled: Vote.CANCELED,
    TaskState.Completed: Vote.UNEXPECTED,
}


@dataclass
class ParticipantOutcome:
    participant: CommitParticipant
    vote: Vote
    #: 计入这一票的那条回执（裁决前定格）。没有表态的为 None
    reply: Optional[TaskResult] = None
    #: 没发成、超时时对方的最后状态、否决理由等
    detail: str = ""
    #: 裁决后对它发了什么："complete" / "cancel" / None（没发，原因见 decision_note）
    decision: Optional[str] = None
    decision_note: str = ""

    @property
    def partner_aic(self) -> str:
        return self.participant.partner_aic

    @property
    def task_id(self) -> str:
        return self.participant.task_id

    @property
    def accepted(self) -> bool:
        return self.vote == Vote.ACCEPTED


@dataclass
class CommitOutcome:
    session_id: str
    participants: List[ParticipantOutcome] = field(default_factory=list)
    #: None = 还没裁决；True = 已对全部参与方发 Complete；False = 已撤回
    committed: Optional[bool] = None

    @property
    def all_accepted(self) -> bool:
        return bool(self.participants) and all(p.accepted for p in self.participants)

    @property
    def accepted(self) -> List[ParticipantOutcome]:
        return [p for p in self.participants if p.accepted]

    @property
    def declined(self) -> List[ParticipantOutcome]:
        return [p for p in self.participants if not p.accepted]

    @property
    def compensation_failed(self) -> List[ParticipantOutcome]:
        """撤回时已经自己完成、撤不回的（规范表 #15 等待超时自动完成）。"""
        return [
            p
            for p in self.participants
            if self.committed is False and p.decision is None and p.decision_note.startswith("already completed")
        ]

    def of(self, partner_aic: str, task_id: Optional[str] = None) -> Optional[ParticipantOutcome]:
        for p in self.participants:
            if p.partner_aic == partner_aic and (task_id is None or p.task_id == task_id):
                return p
        return None


Vet = Callable[[CommitParticipant, TaskResult], Union[Optional[str], Awaitable[Optional[str]]]]
ReplyFilter = Callable[[CommitParticipant, TaskResult], bool]


class GroupCommit:
    """在一个群组会话上做「全体确认才生效、任一不成则撤回」。

    :param leader: 已建好会话的 :class:`~acps_sdk.aip.aip_group_leader.GroupLeader`
    :param session_id: 群组会话号
    :param claim_timeout: 发 Continue 之前，最多等对方说完上一轮（转到 AwaitingInput / AwaitingCompletion）多久
    :param poll_interval: 等待时检查的间隔（秒）
    """

    def __init__(
        self,
        leader: "GroupLeader",
        session_id: str,
        *,
        claim_timeout: float = 30.0,
        poll_interval: float = 0.05,
    ) -> None:
        self.leader = leader
        self.session_id = session_id
        self.claim_timeout = claim_timeout
        self.poll_interval = poll_interval
        self._latest: Dict[_Key, Optional[TaskState]] = {}

    # ------------------------------------------------------------------
    def _session(self) -> "GroupLeaderSession":
        session = self.leader.group_sessions.get(self.session_id)
        if session is None:
            raise ValueError(f"Group session not found: {self.session_id}")
        return session

    def state_of(self, partner_aic: str, task_id: str) -> Optional[TaskState]:
        """这个参与方这个任务最近一次报过的状态（以会话记的为准；会话里没有时用本协调器看到的）。"""
        session = self.leader.group_sessions.get(self.session_id)
        state = session.task_states.get(task_id, {}).get(partner_aic) if session is not None else None
        return state if state is not None else self._latest.get((partner_aic, task_id))

    # ------------------------------------------------------------------
    async def prepare(
        self,
        participants: Sequence[CommitParticipant],
        *,
        timeout: float,
        vet: Optional[Vet] = None,
        reply_filter: Optional[ReplyFilter] = None,
    ) -> CommitOutcome:
        """发提案、收齐表态（或到时），返回每一家的结果。不裁决。

        :param timeout: 提案全部发出后，最多等表态多久（秒）
        :param vet: 可选。对回了 AwaitingCompletion 的每一家调用 ``vet(participant, reply)``（可以是协程），
            返回非空字符串即否决（:attr:`Vote.VETOED`，理由记在 detail），例如产出物和提案对不上
        :param reply_filter: 可选。表了态的回执是否属于这一轮（例如核对回执里回显的提案版本）；返回 False 的不计票
        """
        keys = [p.key for p in participants]
        if len(set(keys)) != len(keys):
            raise ValueError("each (partner_aic, task_id) may appear only once")
        session = self._session()
        loop = asyncio.get_running_loop()
        by_key = {p.key: p for p in participants}
        for p in participants:
            self._latest[p.key] = session.task_states.get(p.task_id, {}).get(p.partner_aic)

        armed: set[_Key] = set()
        votes: Dict[_Key, TaskResult] = {}
        results: Dict[_Key, ParticipantOutcome] = {}

        def on_message(message) -> None:
            if not isinstance(message, TaskResult) or message.status is None:
                return
            key = (str(message.senderId), str(message.taskId))
            p = by_key.get(key)
            if p is None:
                return
            self._latest[key] = message.status.state
            if key in armed and key not in votes and message.status.state in _DECISIVE:
                if reply_filter is not None and not reply_filter(p, message):
                    return
                votes[key] = message

        remove = session.add_message_listener(on_message)
        try:

            async def send(p: CommitParticipant) -> None:
                command = TaskCommandType.Start if p.start else TaskCommandType.Continue
                deadline = loop.time() + self.claim_timeout
                while not command_is_valid(command, self._latest.get(p.key)):
                    state = self._latest.get(p.key)
                    if is_final(state) or (p.start and state is not None) or loop.time() >= deadline:
                        results[p.key] = ParticipantOutcome(
                            p,
                            Vote.NOT_SENT,
                            detail=f"task is {state.value if state else 'absent'}; "
                            f"{command.value} is not valid there",
                        )
                        return
                    await asyncio.sleep(self.poll_interval)
                armed.add(p.key)  # 先布防再发：回得快的那条也不会漏
                try:
                    await session.leader_mq_client.send_task_command(
                        command=command,
                        task_id=p.task_id,
                        session_id=self.session_id,
                        text_content=p.text,
                        data_items=list(p.data_items or []),
                        mentions=[p.partner_aic],
                    )
                except Exception as exc:
                    armed.discard(p.key)
                    results[p.key] = ParticipantOutcome(p, Vote.SEND_FAILED, detail=str(exc))
                    return
                logger.info(
                    "event=group_commit_proposed session_id=%s partner=%s task_id=%s command=%s",
                    self.session_id,
                    p.partner_aic,
                    p.task_id,
                    command.value,
                )

            await asyncio.gather(*(send(p) for p in participants))

            end = loop.time() + timeout
            while any(k in armed and k not in votes for k in keys) and loop.time() < end:
                await asyncio.sleep(self.poll_interval)
        finally:
            remove()

        # 票在这里定格：之后的回执（包括撤回引起的 canceled）不再改写结果
        frozen = dict(votes)
        for p in participants:
            if p.key in results:
                continue
            reply = frozen.get(p.key)
            if reply is None:
                last = self._latest.get(p.key)
                results[p.key] = ParticipantOutcome(
                    p, Vote.TIMEOUT, detail=f"no decision within {timeout}s (last state: {last.value if last else 'none'})"
                )
            else:
                results[p.key] = ParticipantOutcome(p, _VOTE_OF_STATE[reply.status.state], reply=reply)

        if vet is not None:

            async def check(po: ParticipantOutcome) -> None:
                verdict = vet(po.participant, po.reply)  # type: ignore[arg-type]
                if inspect.isawaitable(verdict):
                    verdict = await verdict
                if verdict:
                    po.vote, po.detail = Vote.VETOED, str(verdict)

            await asyncio.gather(*(check(po) for po in results.values() if po.vote == Vote.ACCEPTED))

        outcome = CommitOutcome(self.session_id, [results[p.key] for p in participants])
        logger.info(
            "event=group_commit_prepared session_id=%s accepted=%d total=%d",
            self.session_id,
            len(outcome.accepted),
            len(outcome.participants),
        )
        return outcome

    async def commit(self, outcome: CommitOutcome) -> CommitOutcome:
        """全部同意时，对每一家发 Complete。有一家没同意就抛 ValueError（该调 :meth:`abort`）。"""
        if outcome.committed is not None:
            raise ValueError("outcome has already been decided")
        if not outcome.all_accepted:
            raise ValueError("not every participant accepted; call abort() instead")
        for po in outcome.participants:
            state = self.state_of(po.partner_aic, po.task_id)
            if is_final(state):
                # 规范里 AwaitingCompletion 只会被 Leader 的命令或表 #15 的自动完成带走：已经终结的不再发
                po.decision_note = f"task is {state.value}; Complete not sent"
                continue
            # 不按「最新状态」判断能不能发：这一票是它在提案之后报的 AwaitingCompletion，之后只有 Leader 的 Continue
            # 能让它回到 Working。最新状态若是 Working，只能是早发晚到的旧回执（结果乱序），照样发 Complete
            await self.leader.complete_task(
                session_id=self.session_id, task_id=po.task_id, target_partner=po.partner_aic
            )
            po.decision = "complete"
        outcome.committed = True
        logger.info("event=group_commit_committed session_id=%s", self.session_id)
        return outcome

    async def abort(
        self,
        outcome: CommitOutcome,
        reason: str,
        *,
        include_declined: bool = True,
    ) -> CommitOutcome:
        """撤回：对同意了的（以及 ``include_declined`` 时其余还没终结的）每一家发 Cancel，附上理由。

        已经终结的不再发；其中已经自己完成的（规范表 #15）记进 :attr:`CommitOutcome.compensation_failed`。
        """
        if outcome.committed is not None:
            raise ValueError("outcome has already been decided")
        for po in outcome.participants:
            if not po.accepted and po.vote != Vote.VETOED and not include_declined:
                continue
            state = self.state_of(po.partner_aic, po.task_id)
            if state == TaskState.Completed:
                po.decision_note = "already completed; cannot be canceled"
                logger.warning(
                    "event=group_commit_compensation_failed session_id=%s partner=%s task_id=%s",
                    self.session_id,
                    po.partner_aic,
                    po.task_id,
                )
                continue
            if not command_is_valid(TaskCommandType.Cancel, state):
                po.decision_note = f"task is {state.value if state else 'absent'}; Cancel not sent"
                continue
            await self.leader.cancel_task(
                session_id=self.session_id,
                task_id=po.task_id,
                reason=reason,
                target_partner=po.partner_aic,
            )
            po.decision = "cancel"
        outcome.committed = False
        logger.info("event=group_commit_aborted session_id=%s reason=%s", self.session_id, reason)
        return outcome

    async def run(
        self,
        participants: Sequence[CommitParticipant],
        *,
        timeout: float,
        vet: Optional[Vet] = None,
        reply_filter: Optional[ReplyFilter] = None,
        abort_reason: str = "Not every participant confirmed; the proposal is withdrawn.",
    ) -> CommitOutcome:
        """:meth:`prepare`，然后全部同意就 :meth:`commit`，否则 :meth:`abort`。"""
        outcome = await self.prepare(participants, timeout=timeout, vet=vet, reply_filter=reply_filter)
        if outcome.all_accepted:
            return await self.commit(outcome)
        return await self.abort(outcome, abort_reason)
