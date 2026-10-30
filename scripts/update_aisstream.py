#!/usr/bin/env python3
"""Capture a short AISStream snapshot for Dock Sud and publish safe position data."""
import asyncio
import collections
import datetime as dt
import json
import os
from pathlib import Path

import websockets

KEY = os.environ.get("AISSTREAM_API_KEY")
if not KEY:
    raise SystemExit("Falta AISSTREAM_API_KEY en GitHub Actions.")
BOX = [[[-35.15, -59.05], [-34.30, -57.65]]]
OUTPUT = Path("data/ais.json")
WINDOW_SECONDS = 120
MAX_AGE_SECONDS = 30 * 60


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def parse_time(value, fallback):
    if not value:
        return fallback
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except (ValueError, TypeError):
        return fallback


def position_from(event):
    metadata = event.get("MetaData") or {}
    message_type = event.get("MessageType", "Unknown")
    message = event.get("Message") or {}
    payload = message.get(message_type) or {}
    try:
        lat_value = metadata.get("latitude")
        lon_value = metadata.get("longitude")
        lat = float(payload.get("Latitude") if lat_value is None else lat_value)
        lon = float(payload.get("Longitude") if lon_value is None else lon_value)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    mmsi = metadata.get("MMSI", payload.get("UserID"))
    if not mmsi:
        return None
    now = utc_now()
    return {
        "mmsi": str(mmsi),
        "name": str(metadata.get("ShipName") or "").strip() or "Buque sin nombre",
        "lat": lat,
        "lon": lon,
        "last_seen": parse_time(metadata.get("time_utc"), now).isoformat(),
        "message_type": message_type,
    }


async def capture():
    latest = {}
    counts = collections.Counter()
    async with websockets.connect(
        "wss://stream.aisstream.io/v0/stream",
        compression="deflate",
        open_timeout=15,
        close_timeout=5,
    ) as ws:
        await ws.send(json.dumps({
            "APIKey": KEY,
            "BoundingBoxes": BOX,
            "FilterMessageTypes": [
                "PositionReport",
                "StandardClassBPositionReport",
                "ExtendedClassBPositionReport",
            ],
        }))
        deadline = asyncio.get_running_loop().time() + WINDOW_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                event = json.loads(await asyncio.wait_for(ws.recv(), timeout=min(15, max(0.1, remaining))))
            except asyncio.TimeoutError:
                continue
            if event.get("MessageType") == "SubscriptionConfirmation":
                print("Suscripción AIS aceptada.")
                continue
            counts[event.get("MessageType", "Unknown")] += 1
            vessel = position_from(event)
            if vessel:
                latest[vessel["mmsi"]] = vessel
    return latest, counts


async def main():
    current, counts = await capture()
    now = utc_now()
    previous = {}
    if OUTPUT.exists():
        try:
            old = json.loads(OUTPUT.read_text(encoding="utf-8"))
            for vessel in old.get("vessels", []):
                seen = parse_time(vessel.get("last_seen"), now - dt.timedelta(days=1))
                if (now - seen).total_seconds() <= MAX_AGE_SECONDS:
                    previous[str(vessel.get("mmsi"))] = vessel
        except (OSError, json.JSONDecodeError, AttributeError):
            pass
    previous.update(current)
    result = {
        "updated_at": now.isoformat(),
        "source": "AISStream",
        "area": "Dock Sud / Río de la Plata",
        "timezone": "America/Argentina/Buenos_Aires",
        "window_seconds": WINDOW_SECONDS,
        "message_count": sum(counts.values()),
        "messages": dict(counts),
        "unique_mmsi_in_window": len(current),
        "coverage_confirmed": bool(current),
        "vessels": sorted(previous.values(), key=lambda vessel: vessel["name"].casefold()),
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT.with_suffix(".json.tmp")
    temp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(OUTPUT)
    print(json.dumps({
        "updated_at": result["updated_at"],
        "message_count": result["message_count"],
        "unique_mmsi_in_window": len(current),
        "coverage_confirmed": result["coverage_confirmed"],
        "published_positions": len(result["vessels"]),
    }, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
