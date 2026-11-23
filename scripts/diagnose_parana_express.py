"""Targeted AISStream test for PARANA EXPRESS; no credentials or unverified positions published."""
import asyncio
import json
import os
import time
import websockets

MMSI = "563282300"
IMO = "9793909"
URL = "wss://stream.aisstream.io/v0/stream"
BOXES = [
    [[-35.20, -59.80], [-33.70, -57.00]],  # Buenos Aires / Dock Sud
    [[-37.50, -60.50], [-32.00, -52.00]],  # Rio de la Plata and Atlantic approaches
]

async def main():
    key = os.getenv("AISSTREAM_API_KEY", "").strip()
    if not key:
        raise SystemExit("AISSTREAM_API_KEY missing")
    started = time.monotonic()
    print("Target MMSI:", MMSI, "window: 150 seconds", flush=True)
    seen = 0
    confirmations = 0
    target = 0
    imo_verified_in_stream = False
    try:
        async with websockets.connect(URL, open_timeout=40, close_timeout=5, ping_interval=20) as ws:
            print("Handshake OK after", round(time.monotonic()-started, 1), "seconds", flush=True)
            await ws.send(json.dumps({
                "APIKey": key,
                "BoundingBoxes": BOXES,
                "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport", "ShipStaticData", "StaticDataReport"],
            }))
            print("Regional subscription sent; local MMSI filter and IMO cross-check", flush=True)
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline:
                try:
                    event = json.loads(await asyncio.wait_for(ws.recv(), timeout=min(15, max(.1, deadline-time.monotonic()))))
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed as exc:
                    print("AISStream disconnected before deadline:", type(exc).__name__, flush=True)
                    break
                typ = event.get("MessageType", "")
                if typ == "SubscriptionConfirmation":
                    confirmations += 1
                    print("Subscription confirmed", flush=True)
                    continue
                if typ not in {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport", "ShipStaticData", "StaticDataReport"}:
                    print("Other message type:", typ, flush=True)
                    continue
                metadata = event.get("MetaData") or {}
                payload = (event.get("Message") or {}).get(typ) or {}
                mmsi = str(metadata.get("MMSI") or payload.get("UserID") or "")
                if mmsi != MMSI:
                    continue
                if typ in {"ShipStaticData", "StaticDataReport"}:
                    reported_imo = str(payload.get("ImoNumber") or payload.get("IMONumber") or payload.get("IMO") or "").strip()
                    if reported_imo:
                        imo_verified_in_stream = reported_imo == IMO
                        print("TARGET STATIC ID:", json.dumps({"mmsi": MMSI, "reported_imo": reported_imo, "expected_imo": IMO, "imo_match": imo_verified_in_stream}), flush=True)
                    continue
                seen += 1
                target += 1
                lat = payload.get("Latitude", metadata.get("Latitude"))
                lon = payload.get("Longitude", metadata.get("Longitude"))
                print("TARGET FOUND:", json.dumps({"mmsi": mmsi, "lat": lat, "lon": lon, "timestamp": metadata.get("time_utc"), "type": typ}), flush=True)
            print("RESULT:", json.dumps({"expected_imo": IMO, "expected_mmsi": MMSI, "imo_verified_in_stream": imo_verified_in_stream, "subscription_confirmations": confirmations, "position_messages_in_region": seen, "target_mmsi_messages": target, "target_found": bool(target)}), flush=True)
    except Exception as exc:
        print("Connection error:", type(exc).__name__, str(exc).replace(key, "[redacted]")[:200], flush=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
