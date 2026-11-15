"""在真实 RabbitMQ 上跑 GroupCommit（只用 SDK）。

几家标准的 GroupPartnerMqClient：收到 Start / Continue 照脚本回 AwaitingCompletion（同意）、AwaitingInput（要改）或不回；
收到 Complete / Cancel 照规范转 Completed / Canceled。Leader 经直接邀请（RPC）把它们请进群。

场景：
  1 全部同意 → 统一完成
  2 一家要改 → 统一撤回（含已同意的）；第 2 版改过再提 → 全部同意 → 完成（同一批任务，Continue）
  3 一家不回 → 到时撤回，取消送到还在工作中的那一家
需要一台用户名口令认证的 RabbitMQ：
  docker run -d --name gc-rmq -p 127.0.0.1:35673:5672 -e RABBITMQ_DEFAULT_USER=repro -e RABBITMQ_DEFAULT_PASS=repro rabbitmq:4-management
用法：python group_commit_real_broker.py --port 35673
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import uuid
from importlib import metadata

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from acps_sdk.aip.aip_base_model import Product, TaskCommandType, TaskState, TextDataItem
from acps_sdk.aip.aip_group_commit import CommitParticipant, GroupCommit, Vote
from acps_sdk.aip.aip_group_leader import GroupLeader
from acps_sdk.aip.aip_group_model import ACSObject, RabbitMQRequest
from acps_sdk.aip.aip_group_partner import GroupPartnerMqClient, PartnerGroupState

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=35673)
args = ap.parse_args()
MQ = dict(host="127.0.0.1", port=args.port, vhost="/", user="repro", password="repro")
LEADER = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
AICS = ["1.2.156.3088.1.1.34C2.478BDF.3GF547.0JU5", "1.2.156.3088.1.1.34C2.478BDF.3GF548.0JU6",
        "1.2.156.3088.1.1.34C2.478BDF.3GF549.0JU7"]


class Warn(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.records: list[str] = []

    def emit(self, record):
        msg = record.getMessage()
        if "identity binding disabled" in msg:
            return
        self.records.append(f"{record.name}: {msg[:140]}")


warn = Warn()
logging.getLogger().addHandler(warn)
logging.getLogger().setLevel(logging.WARNING)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Member:
    """一家标准 Partner：behaviour(版本号) → "accept" / "change" / "silent"。"""

    def __init__(self, aic: str, behaviour, leave_on_complete: bool = False):
        self.aic, self.behaviour, self.leave_on_complete = aic, behaviour, leave_on_complete
        self.client = GroupPartnerMqClient(partner_aic=aic, rabbitmq_host=MQ["host"], rabbitmq_port=MQ["port"],
                                           rabbitmq_vhost="/", identity_binding_enabled=False)
        self.client.set_command_handler(self.on_command)
        self.states: dict[str, TaskState] = {}
        self.round = 0
        self.left = 0
        self.client.set_disconnect_handler(lambda _c, _g: setattr(self, "left", self.left + 1))

    async def on_command(self, command, is_mentioned) -> None:
        if not is_mentioned:
            return
        tid, sid, c = command.taskId, command.sessionId, command.command
        if c in (TaskCommandType.Start, TaskCommandType.Continue):
            if c == TaskCommandType.Start:
                await self.client.accept_task(tid, sid)
            self.round += 1
            await self.client.start_working(tid, sid)
            self.states[tid] = TaskState.Working
            how = self.behaviour(self.round)
            await asyncio.sleep(0.2)                      # 想一想
            if how == "accept":
                await self.client.submit_for_completion(tid, sid, [Product(
                    id=f"p-{uuid.uuid4().hex[:6]}", dataItems=[TextDataItem(text=f"{self.aic[-4:]} 确认单（第 {self.round} 轮）")])])
                self.states[tid] = TaskState.AwaitingCompletion
            elif how == "change":
                await self.client.request_input(tid, sid, "价钱要改成 1200 才能接")
                self.states[tid] = TaskState.AwaitingInput
        elif c == TaskCommandType.Complete:
            await self.client.complete_task(tid, sid)
            self.states[tid] = TaskState.Completed
            if self.leave_on_complete:
                await self.client.leave_group(session_id=sid)
        elif c == TaskCommandType.Cancel:
            await self.client.cancel_task(tid, sid)
            self.states[tid] = TaskState.Canceled


async def serve_member(m: Member):
    async def endpoint(request: Request):
        req = RabbitMQRequest.model_validate(await request.json())
        resp = await m.client.join_group(req)
        return JSONResponse(resp.model_dump(exclude_none=True))
    app = Starlette(routes=[Route("/aip/group/rpc", endpoint, methods=["POST"])])
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    return server, task, f"http://127.0.0.1:{port}/aip/group/rpc"


async def scenario(name: str, behaviours, *, timeout: float = 5.0, revise: bool = False, leave_on_complete=False):
    warn.records.clear()
    members = [Member(a, b, leave_on_complete) for a, b in zip(AICS, behaviours)]
    servers = [await serve_member(m) for m in members]
    leader = GroupLeader(leader_aic=LEADER, rabbitmq_config=MQ, identity_binding_enabled=False)
    sid = f"s-{uuid.uuid4().hex[:6]}"
    await leader.create_group_session(sid, [ACSObject(aic=m.aic) for m in members])
    for m, (_, _, url) in zip(members, servers):
        await leader.invite_partner(sid, ACSObject(aic=m.aic), partner_rpc_url=url)
    await asyncio.sleep(0.5)
    commit = GroupCommit(leader, sid)
    tasks = {m.aic: f"task-{uuid.uuid4().hex[:6]}" for m in members}
    t0 = asyncio.get_running_loop().time()
    parts = [CommitParticipant(m.aic, tasks[m.aic], text="第 1 版：请确认能否承接", start=True) for m in members]
    out = await commit.prepare(parts, timeout=timeout)
    line = [f"第1版 " + " ".join(f"{p.partner_aic[-4:]}={p.vote.value}" for p in out.participants)]
    if out.all_accepted:
        await commit.commit(out)
    elif revise:
        # 照要改的那家改了，再提第 2 版：同一批任务，Continue（等待输入 / 等待完成下都有效）
        parts = [CommitParticipant(m.aic, tasks[m.aic], text="第 2 版：价钱照你说的改成 1200") for m in members]
        out = await commit.prepare(parts, timeout=timeout)
        line.append("第2版 " + " ".join(f"{p.partner_aic[-4:]}={p.vote.value}" for p in out.participants))
        await (commit.commit(out) if out.all_accepted else commit.abort(out, "没锁成"))
    else:
        await commit.abort(out, "有一家没确认，撤回")
    took = asyncio.get_running_loop().time() - t0
    await asyncio.sleep(0.8)
    finals = " ".join(f"{m.aic[-4:]}:{m.states.get(tasks[m.aic]).value}/{(leader.group_sessions[sid].task_states.get(tasks[m.aic], {}).get(m.aic) or TaskState.Working).value}"
                      for m in members)
    decisions = " ".join(f"{p.partner_aic[-4:]}→{p.decision}" for p in out.participants)
    states = [m.client.state.value for m in members]
    # 成员先主动退群、再解散（v2.2.0 里成员收到 DISBAND 后会停在 leaving，见 #37）
    for m in members:
        await asyncio.wait_for(m.client.close(), 5)
    await leader.dissolve_group_session(sid)
    after = [m.client.state.value for m in members]
    await leader.close()
    for server, task, _ in servers:
        server.should_exit = True
        await task
    print(f"[{name}] committed={out.committed} 用时 {took:.1f}s  " + " | ".join(line))
    print(f"    裁决: {decisions}")
    print(f"    成员本地/Leader 记的最终状态: {finals}")
    print(f"    裁决后成员连接: {states} → 退群、解散后 {after}；断开回调次数 {[m.left for m in members]}；WARNING+={len(warn.records)}")
    for r in warn.records:
        print("      ", r)


async def main():
    print("acps-sdk", metadata.version("acps-sdk"))
    acc = lambda r: "accept"
    await scenario("全部同意", [acc, acc, acc])
    await scenario("一家要改→撤回", [acc, lambda r: "change", acc])
    await scenario("一家要改→第2版", [acc, lambda r: "change" if r == 1 else "accept", acc], revise=True)
    await scenario("一家不回→超时", [acc, acc, lambda r: "silent"], timeout=2.0)


asyncio.run(main())
