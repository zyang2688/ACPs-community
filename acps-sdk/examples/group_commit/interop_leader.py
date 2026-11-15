"""互通：Leader 用带 GroupCommit 的 SDK，成员在另一个进程里用未修改的 2.2.0（interop_partners.py）。

用法：python interop_leader.py <装了原版 2.2.0 的 python> <RabbitMQ 端口>
"""
import asyncio
import subprocess
import sys
import uuid
from importlib import metadata

from acps_sdk.aip.aip_base_model import TaskState
from acps_sdk.aip.aip_group_commit import CommitParticipant, GroupCommit
from acps_sdk.aip.aip_group_leader import GroupLeader
from acps_sdk.aip.aip_group_model import ACSObject

PARTNER_PY, PORT = sys.argv[1], int(sys.argv[2])
MQ = dict(host="127.0.0.1", port=PORT, vhost="/", user="repro", password="repro")
LEADER = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"


async def scenario(urls, revise: bool):
    leader = GroupLeader(leader_aic=LEADER, rabbitmq_config=MQ, identity_binding_enabled=False)
    sid = f"s-{uuid.uuid4().hex[:6]}"
    await leader.create_group_session(sid, [ACSObject(aic=a) for a in urls])
    for aic, url in urls.items():
        await leader.invite_partner(sid, ACSObject(aic=aic), partner_rpc_url=url)
    await asyncio.sleep(0.5)
    commit = GroupCommit(leader, sid)
    tasks = {a: f"task-{uuid.uuid4().hex[:6]}" for a in urls}
    out = await commit.prepare([CommitParticipant(a, tasks[a], text="第 1 版", start=True) for a in urls], timeout=5)
    votes = ["v1 " + " ".join(f"{p.partner_aic[-4:]}={p.vote.value}" for p in out.participants)]
    if revise and not out.all_accepted:
        out = await commit.prepare([CommitParticipant(a, tasks[a], text="第 2 版：改成 1200") for a in urls], timeout=5)
        votes.append("v2 " + " ".join(f"{p.partner_aic[-4:]}={p.vote.value}" for p in out.participants))
    await (commit.commit(out) if out.all_accepted else commit.abort(out, "没锁成"))
    await asyncio.sleep(0.8)
    session = leader.group_sessions[sid]
    finals = " ".join(f"{a[-4:]}:{(session.task_states.get(tasks[a], {}).get(a) or TaskState.Working).value}" for a in urls)
    await leader.dissolve_group_session(sid)
    await leader.close()
    print(f"  revise={revise} committed={out.committed}  {' | '.join(votes)}  → Leader 看到的最终状态 {finals}")


async def main():
    proc = subprocess.Popen([PARTNER_PY, "-I", __file__.replace("interop_leader.py", "interop_partners.py"), str(PORT)],
                            stdout=subprocess.PIPE, text=True)
    urls = {}
    for line in proc.stdout:
        print("  partners:", line.strip()[:120])
        if line.startswith("READY "):
            urls = dict(x.split("=", 1) for x in line.split()[1:])
            break
    print("leader SDK", metadata.version("acps-sdk"))
    try:
        await scenario(urls, revise=True)
        await scenario(urls, revise=False)
    finally:
        proc.terminate()


asyncio.run(main())
