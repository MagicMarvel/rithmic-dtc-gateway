"""Authenticated Web terminal WebSocket smoke test.

Credentials are read from environment variables and are never printed.
"""

import argparse
import asyncio
import json
import os
import ssl
import urllib.request

import websockets


def login(base_url: str, insecure: bool) -> str:
    payload = json.dumps(
        {
            "username": os.environ["TERMINAL_MEMBER_USER"],
            "password": os.environ["TERMINAL_MEMBER_PASSWORD"],
        }
    ).encode()
    request = urllib.request.Request(
        f"{base_url}/api/auth/login",
        data=payload,
        headers={"content-type": "application/json"},
    )
    context = ssl._create_unverified_context() if insecure else None
    with urllib.request.urlopen(request, timeout=15, context=context) as response:
        cookie = response.headers.get("set-cookie", "").split(";", 1)[0]
    if not cookie:
        raise RuntimeError("member login did not return a session cookie")
    return cookie


async def smoke(
    base_url: str, symbol: str, exchange: str, tick: float, insecure: bool
) -> None:
    cookie = await asyncio.to_thread(login, base_url, insecure)
    ws_url = base_url.replace("https://", "wss://").replace("http://", "ws://") + "/ws"
    context = None
    if ws_url.startswith("wss://"):
        context = (
            ssl._create_unverified_context() if insecure else ssl.create_default_context()
        )
    async with websockets.connect(
        ws_url,
        ssl=context,
        additional_headers={"Cookie": cookie},
        open_timeout=15,
    ) as socket:
        await socket.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "symbol": symbol,
                    "exchange": exchange,
                    "tick": tick,
                }
            )
        )
        found = set()
        for _ in range(80):
            message = json.loads(await asyncio.wait_for(socket.recv(), timeout=15))
            kind = message.get("type")
            if kind:
                found.add(kind)
            if "quote" in found and ({"depth", "depthLevel", "depthUpdate"} & found):
                break
        print(json.dumps({"messageTypes": sorted(found)}, ensure_ascii=False))
        if "quote" not in found:
            raise RuntimeError("real-time quote was not received")
        if not ({"depth", "depthLevel", "depthUpdate"} & found):
            raise RuntimeError("Market Depth was not received")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url")
    parser.add_argument("--symbol", default="ESZ6")
    parser.add_argument("--exchange", default="CME")
    parser.add_argument("--tick", type=float, default=0.25)
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="skip TLS verification for direct-IP deployment diagnostics only",
    )
    args = parser.parse_args()
    asyncio.run(
        smoke(
            args.base_url.rstrip("/"),
            args.symbol,
            args.exchange,
            args.tick,
            args.insecure,
        )
    )
