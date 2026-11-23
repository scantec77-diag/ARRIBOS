"""Minimal AISStream handshake diagnostic. Never prints API credentials."""
import asyncio
import json
import os
import sys
import time

import websockets

URL = "wss://stream.aisstream.io/v0/stream"

async def main():
    key = os.getenv("AISSTREAM_API_KEY", "").strip()
    if not key:
        print("ERROR: AISSTREAM_API_KEY secret is missing or empty")
        return 2
    print("Connecting to AISStream (30s handshake timeout)...", flush=True)
    start = time.monotonic()
    try:
        async with websockets.connect(URL, open_timeout=30, close_timeout=5, ping_interval=20) as ws:
            print(f"WebSocket handshake OK in {time.monotonic()-start:.1f}s", flush=True)
            await ws.send(json.dumps({
                "APIKey": key,
                "BoundingBoxes": [[[-35.5, -59.5], [-33.5, -57.0]]],
                "FilterMessageTypes": ["PositionReport"],
            }))
            print("Subscription sent; waiting up to 45s for response...", flush=True)
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=45)
                try:
                    payload = json.loads(message)
                    print("Received message type:", payload.get("MessageType", "unknown"), flush=True)
                except (ValueError, TypeError):
                    print("Received non-JSON message (content omitted)", flush=True)
                return 0
            except asyncio.TimeoutError:
                print("Handshake OK; no AIS messages received within 45s", flush=True)
                return 3
    except Exception as exc:
        print(f"Connection failed: {type(exc).__name__}: {str(exc)[:250]}", flush=True)
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
