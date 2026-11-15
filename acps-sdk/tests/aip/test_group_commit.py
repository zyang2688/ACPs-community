"""GroupCommit：不需要 RabbitMQ，用真的 GroupLeader / GroupLeaderSession，发出去的消息由脚本化的参与方回应，
回应经 GroupLeader._handle_group_message 走和真实消费回调一样的路由。"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import pytest

from acps_sdk.aip.aip_base_model import (
    Product,
    StructuredDataItem,
    TaskCommand,
    TaskCommandType,
    TaskResult,
    TaskState,
    TaskStatus,
    TextDataItem,
)
from acps_sdk.aip.aip_group_commit import CommitParticipant, GroupCommit, Vote
from acps_sdk.aip.aip_group_leader import (
    GroupLeader,
    GroupLeaderMqClient,
    GroupLeaderSession,
    GroupLeaderState,
)

LEADER = "leader-aic"
A, B, P = "partner-a", "partner-b", "platform-p"
S = TaskState

#: 参与方脚本：收到一条命令 → [(延迟秒, 状态, 文字), ...]
Script = Callable[[TaskCommand], List[Tuple[float, TaskState, str]]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Harness:
    def __init__(self) -> None:
        self.leader = GroupLeader(LEADER, {"host": "mq.example", "port": 5671, "vhost": "acps"})
        self.client = GroupLeaderMqClient(leader_aic=LEADER)
        self.client._state = GroupLeaderState.READY
        self.client._group_id = "group-1"
        self.session = GroupLeaderSession("session-1", "group-1", self.client)
        self.leader.group_sessions["session-1"] = self.session
        self.sent: List[TaskCommand] = []
        self.sent_times: List[float] = []
        self.scripts: Dict[str, Script] = {}
        self._tasks: set = set()
        self.client.publish_message = self._publish  # type: ignore[assignment]

    async def _publish(self, message, *args, **kwargs) -> None:
        if not isinstance(message, TaskCommand):
            return
        self.sent.append(message)
        self.sent_times.append(asyncio.get_running_loop().time())
        target = message.mentions[0] if isinstance(message.mentions, list) and message.mentions else None
        script = self.scripts.get(target or "")
        if script is None:
            return
        for delay, state, text in script(message):
            self._spawn(self.reply(target, message.taskId, state, text, delay))

    def _spawn(self, coro) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def reply(self, partner: str, task_id: str, state: TaskState, text: str = "", delay: float = 0.0,
                    data: Optional[dict] = None) -> None:
        if delay:
            await asyncio.sleep(delay)
        items = [TextDataItem(text=text)] if text else []
        if data is not None:
            items.append(StructuredDataItem(data=data))
        products = [Product(id="p1", dataItems=[TextDataItem(text=text or "ok")])] if state == S.AwaitingCompletion else None
        result = TaskResult(
            id=f"r-{partner}-{task_id}-{state.value}", sentAt=_now(), senderRole="partner", senderId=partner,
            taskId=task_id, sessionId="session-1", groupId="group-1",
            status=TaskStatus(state=state, stateChangedAt=_now(), dataItems=items or None), products=products,
        )
        await self.leader._handle_group_message(result)

    def commands(self, partner: str) -> List[str]:
        return [c.command.value for c in self.sent if c.mentions == [partner]]


def accept(cmd: TaskCommand):
    if cmd.command == TaskCommandType.Start:
        return [(0.0, S.Accepted, ""), (0.01, S.Working, ""), (0.03, S.AwaitingCompletion, "确认单：照办")]
    if cmd.command == TaskCommandType.Continue:
        return [(0.0, S.Working, ""), (0.03, S.AwaitingCompletion, "确认单：照办")]
    if cmd.command == TaskCommandType.Complete:
        return [(0.0, S.Completed, "")]
    if cmd.command == TaskCommandType.Cancel:
        return [(0.0, S.Canceled, "")]
    return []


def wants_change(cmd: TaskCommand):
    if cmd.command == TaskCommandType.Continue:
        return [(0.0, S.Working, ""), (0.02, S.AwaitingInput, "价钱要改成 1200 才能接")]
    return accept(cmd)


def silent(cmd: TaskCommand):
    if cmd.command == TaskCommandType.Continue:
        return [(0.0, S.Working, "")]
    return accept(cmd)


async def _settle(h: Harness) -> None:
    for _ in range(20):
        await asyncio.sleep(0.01)


async def _prime(h: Harness, partner: str, task_id: str, state: TaskState = S.AwaitingInput) -> None:
    await h.reply(partner, task_id, state, "上一轮的话")


@pytest.mark.asyncio
async def test_all_accept_commits_everyone() -> None:
    h = Harness()
    h.scripts = {A: accept, B: accept}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b", S.AwaitingCompletion)
    outcome = await GroupCommit(h.leader, "session-1").run(
        [CommitParticipant(A, "t-a", text="第 2 版"), CommitParticipant(B, "t-b", text="第 2 版")], timeout=2,
    )
    await _settle(h)
    assert outcome.committed is True
    assert [p.vote for p in outcome.participants] == [Vote.ACCEPTED, Vote.ACCEPTED]
    assert h.commands(A) == ["continue", "complete"]
    assert h.commands(B) == ["continue", "complete"]
    assert h.session.task_states["t-a"][A] == S.Completed
    assert "照办" in outcome.of(A).reply.products[0].dataItems[0].text


@pytest.mark.asyncio
async def test_one_declines_aborts_and_votes_stay_frozen() -> None:
    h = Harness()
    h.scripts = {A: accept, B: wants_change}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")
    outcome = await GroupCommit(h.leader, "session-1").run(
        [CommitParticipant(A, "t-a", text="v2"), CommitParticipant(B, "t-b", text="v2")], timeout=2,
    )
    await _settle(h)
    assert outcome.committed is False
    assert outcome.of(A).vote == Vote.ACCEPTED and outcome.of(A).decision == "cancel"
    assert outcome.of(B).vote == Vote.NEEDS_INPUT and outcome.of(B).decision == "cancel"
    assert "1200" in outcome.of(B).reply.status.dataItems[0].text
    # 取消引起的 canceled 回执到了，结果不回头改写
    assert h.session.task_states["t-a"][A] == S.Canceled
    assert outcome.of(A).vote == Vote.ACCEPTED
    assert h.commands(A) == ["continue", "cancel"]


@pytest.mark.asyncio
async def test_waits_for_previous_round_and_ignores_its_late_reply() -> None:
    """对方还在想上一轮（Working）：等它说完再发；它上一轮那句晚到的「等待输入」不算对这一版的表态。"""
    h = Harness()
    h.scripts = {A: accept}
    await h.reply(A, "t-a", S.Working)

    async def finish_previous_round() -> None:
        await asyncio.sleep(0.1)
        await h.reply(A, "t-a", S.AwaitingInput, "（上一轮）收到需求，需要什么请直接问")

    t0 = asyncio.get_running_loop().time()
    h._spawn(finish_previous_round())
    outcome = await GroupCommit(h.leader, "session-1").prepare([CommitParticipant(A, "t-a", text="v1")], timeout=2)
    assert outcome.of(A).vote == Vote.ACCEPTED
    assert h.sent_times[0] - t0 >= 0.1  # 是在它说完上一轮之后才发的
    assert outcome.of(A).reply.status.state == S.AwaitingCompletion


@pytest.mark.asyncio
async def test_timeout_and_cancel_of_silent_member() -> None:
    h = Harness()
    h.scripts = {A: accept, B: silent}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")
    outcome = await GroupCommit(h.leader, "session-1").run(
        [CommitParticipant(A, "t-a"), CommitParticipant(B, "t-b")], timeout=0.3, abort_reason="B 没回",
    )
    await _settle(h)
    assert outcome.of(B).vote == Vote.TIMEOUT and "working" in outcome.of(B).detail
    assert outcome.of(B).decision == "cancel"
    cancel = [c for c in h.sent if c.command == TaskCommandType.Cancel and c.mentions == [B]][0]
    assert cancel.dataItems[0].text == "B 没回"


@pytest.mark.asyncio
async def test_vet_can_veto_an_acceptance() -> None:
    h = Harness()
    h.scripts = {A: accept, B: accept}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")

    async def vet(p: CommitParticipant, reply: TaskResult) -> Optional[str]:
        await asyncio.sleep(0)
        return "确认单上的总价和派给它的不一样" if p.partner_aic == B else None

    outcome = await GroupCommit(h.leader, "session-1").run(
        [CommitParticipant(A, "t-a"), CommitParticipant(B, "t-b")], timeout=2, vet=vet,
    )
    assert outcome.committed is False
    assert outcome.of(B).vote == Vote.VETOED and "总价" in outcome.of(B).detail
    assert {p.decision for p in outcome.participants} == {"cancel"}


@pytest.mark.asyncio
async def test_not_sent_when_task_already_final_and_start_for_new_tasks() -> None:
    h = Harness()
    h.scripts = {A: accept, B: accept}
    await h.reply(B, "t-b", S.Canceled)
    outcome = await GroupCommit(h.leader, "session-1").run(
        [CommitParticipant(A, "t-a-new", text="开门见山的提案", start=True), CommitParticipant(B, "t-b")], timeout=2,
    )
    assert outcome.of(A).vote == Vote.ACCEPTED
    assert h.commands(A)[0] == "start"
    assert outcome.of(B).vote == Vote.NOT_SENT and "canceled" in outcome.of(B).detail
    assert outcome.committed is False
    assert h.commands(B) == []  # 终态的任务什么都不再发


@pytest.mark.asyncio
async def test_same_partner_with_two_tasks_is_two_participants() -> None:
    """平台下两家店：同一个成员 AIC、各自的任务号，各算各的票。"""
    h = Harness()

    def per_task(cmd: TaskCommand):
        return accept(cmd) if cmd.taskId == "shop-1" else wants_change(cmd)

    h.scripts = {P: per_task}
    await _prime(h, P, "shop-1")
    await _prime(h, P, "shop-2")
    outcome = await GroupCommit(h.leader, "session-1").prepare(
        [CommitParticipant(P, "shop-1"), CommitParticipant(P, "shop-2")], timeout=2,
    )
    assert outcome.of(P, "shop-1").vote == Vote.ACCEPTED
    assert outcome.of(P, "shop-2").vote == Vote.NEEDS_INPUT
    with pytest.raises(ValueError):
        await GroupCommit(h.leader, "session-1").prepare(
            [CommitParticipant(P, "shop-1"), CommitParticipant(P, "shop-1")], timeout=1,
        )


@pytest.mark.asyncio
async def test_reply_filter_drops_replies_for_another_version() -> None:
    h = Harness()

    async def stale_then_fresh(cmd: TaskCommand) -> None:
        await h.reply(A, "t-a", S.Working)
        await h.reply(A, "t-a", S.AwaitingCompletion, "第 1 版的确认单", data={"version": 1})
        await asyncio.sleep(0.05)
        await h.reply(A, "t-a", S.Working)
        await h.reply(A, "t-a", S.AwaitingCompletion, "第 2 版的确认单", data={"version": 2})

    h.scripts = {A: lambda cmd: []}
    await _prime(h, A, "t-a")
    original = h._publish

    async def publish(message, *a, **kw):
        await original(message, *a, **kw)
        if isinstance(message, TaskCommand) and message.command == TaskCommandType.Continue:
            h._spawn(stale_then_fresh(message))

    h.client.publish_message = publish  # type: ignore[assignment]

    def same_version(p: CommitParticipant, reply: TaskResult) -> bool:
        versions = [d.data.get("version") for d in reply.status.dataItems or [] if isinstance(d, StructuredDataItem)]
        return versions == [2]

    outcome = await GroupCommit(h.leader, "session-1").prepare(
        [CommitParticipant(A, "t-a", text="第 2 版")], timeout=2, reply_filter=same_version,
    )
    assert outcome.of(A).vote == Vote.ACCEPTED
    assert "第 2 版" in outcome.of(A).reply.status.dataItems[0].text


@pytest.mark.asyncio
async def test_abort_reports_member_that_already_auto_completed() -> None:
    """规范表 #15：等待完成超时后对方自己转成 Completed，那之后撤不回。"""
    h = Harness()
    h.scripts = {A: accept, B: wants_change}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")
    commit = GroupCommit(h.leader, "session-1")
    outcome = await commit.prepare([CommitParticipant(A, "t-a"), CommitParticipant(B, "t-b")], timeout=2)
    await h.reply(A, "t-a", S.Completed)  # 它等不及、自己完成了
    await commit.abort(outcome, "没锁成")
    assert [p.partner_aic for p in outcome.compensation_failed] == [A]
    assert outcome.of(A).decision is None and outcome.of(B).decision == "cancel"
    with pytest.raises(ValueError):
        await commit.commit(outcome)


@pytest.mark.asyncio
async def test_commit_refuses_partial_acceptance() -> None:
    h = Harness()
    h.scripts = {A: accept, B: wants_change}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")
    commit = GroupCommit(h.leader, "session-1")
    outcome = await commit.prepare([CommitParticipant(A, "t-a"), CommitParticipant(B, "t-b")], timeout=2)
    with pytest.raises(ValueError):
        await commit.commit(outcome)
    assert TaskCommandType.Complete not in [c.command for c in h.sent]


@pytest.mark.asyncio
async def test_listener_is_removed_after_prepare() -> None:
    h = Harness()
    h.scripts = {A: accept}
    await _prime(h, A, "t-a")
    await GroupCommit(h.leader, "session-1").prepare([CommitParticipant(A, "t-a")], timeout=2)
    assert h.session._message_listeners == []


@pytest.mark.asyncio
async def test_session_message_listener_add_remove_and_isolation() -> None:
    h = Harness()
    seen: List[str] = []

    def bad(message) -> None:
        raise RuntimeError("listener bug")

    remove = h.session.add_message_listener(lambda m: seen.append(m.status.state.value))
    h.session.add_message_listener(bad)
    await h.reply(A, "t-a", S.Working)
    assert seen == ["working"]
    assert h.session.task_states["t-a"][A] == S.Working  # 监听者抛异常不影响路由
    remove()
    await h.reply(A, "t-a", S.AwaitingInput)
    assert seen == ["working"]


@pytest.mark.asyncio
async def test_commit_is_not_skipped_by_a_late_stale_working_result() -> None:
    """同一任务的结果乱序：较早的 working 在 awaiting-completion 之后才到，会话里记的最新状态成了 working。
    这一票已经定了（提案之后报的等待完成），提交照样发 Complete，不因为乱序漏掉一家。"""
    h = Harness()
    h.scripts = {A: accept, B: accept}
    await _prime(h, A, "t-a")
    await _prime(h, B, "t-b")
    commit = GroupCommit(h.leader, "session-1")
    outcome = await commit.prepare([CommitParticipant(A, "t-a"), CommitParticipant(B, "t-b")], timeout=2)
    await h.reply(B, "t-b", S.Working)  # 早发晚到
    assert h.session.task_states["t-b"][B] == S.Working
    await commit.commit(outcome)
    assert [p.decision for p in outcome.participants] == ["complete", "complete"]
