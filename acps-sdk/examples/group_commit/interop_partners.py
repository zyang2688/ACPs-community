"""互通：三家标准 Partner（跑在另一个进程里，用原版 acps-sdk 2.2.0），等 Leader 的直接邀请。每次邀请现建一个客户端。
A 一律同意；B 第一轮要改、之后同意；C 一律同意。打印 READY <aic>=<url> ... 后一直跑。"""
import asyncio
import json
import socket
import sys
import uuid
from importlib import metadata

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from acps_sdk.aip.aip_base_model import Product, TaskCommandType, TextDataItem
from acps_sdk.aip.aip_group_model import RabbitMQRequest
from acps_sdk.aip.aip_group_partner import GroupPartnerMqClient

PORT = int(sys.argv[1])
AICS = ["1.2.156.3088.1.1.34C2.478BDF.3GF547.0JU5", "1.2.156.3088.1.1.34C2.478BDF.3GF548.0JU6",
        "1.2.156.3088.1.1.34C2.478BDF.3GF549.0JU7"]
CLIENTS = []


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make(aic: str):
    client = GroupPartnerMqClient(partner_aic=aic, rabbitmq_host="127.0.0.1", rabbitmq_port=PORT, rabbitmq_vhost="/",
                                  identity_binding_enabled=False)
    rounds = {"n": 0}

    async def on_command(command, is_mentioned):
        if not is_mentioned:
            return
        tid, sid, c = command.taskId, command.sessionId, command.command
        if c in (TaskCommandType.Start, TaskCommandType.Continue):
            if c == TaskCommandType.Start:
                await client.accept_task(tid, sid)
            rounds["n"] += 1
            await client.start_working(tid, sid)
            await asyncio.sleep(0.2)
            if aic.endswith("0JU6") and rounds["n"] == 1:
                await client.request_input(tid, sid, "价钱要改成 1200 才能接")
            else:
                await client.submit_for_completion(tid, sid, [Product(
                    id=f"p-{uuid.uuid4().hex[:6]}", dataItems=[TextDataItem(text=f"{aic[-4:]} 确认单")])])
        elif c == TaskCommandType.Complete:
            await client.complete_task(tid, sid)
        elif c == TaskCommandType.Cancel:
            await client.cancel_task(tid, sid)

    client.set_command_handler(on_command)
    CLIENTS.append(client)
    return client


async def main():
    urls = []
    servers = []
    for aic in AICS:
        async def endpoint(request: Request, aic=aic):
            req = RabbitMQRequest.model_validate(await request.json())
            resp = await make(aic).join_group(req)
            return JSONResponse(resp.model_dump(exclude_none=True))
        port = free_port()
        app = Starlette(routes=[Route("/aip/group/rpc", endpoint, methods=["POST"])])
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off"))
        servers.append(asyncio.create_task(server.serve()))
        urls.append(f"{aic}=http://127.0.0.1:{port}/aip/group/rpc")
    await asyncio.sleep(0.5)
    print("SDK", metadata.version("acps-sdk"), "has aip_group_commit:", _has_commit(), flush=True)
    print("READY " + " ".join(urls), flush=True)
    await asyncio.gather(*servers)


def _has_commit() -> bool:
    try:
        import acps_sdk.aip.aip_group_commit  # noqa: F401
        return True
    except ImportError:
        return False


asyncio.run(main())
