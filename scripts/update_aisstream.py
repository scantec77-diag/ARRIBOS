#!/usr/bin/env python3
"""Capture AIS positions and publish only ships listed for Exolgan."""
import asyncio
import collections
import datetime as dt
import json
import os
import unicodedata
from pathlib import Path

import websockets

KEY = os.environ.get("AISSTREAM_API_KEY")
if not KEY:
    raise SystemExit("Falta AISSTREAM_API_KEY en GitHub Actions.")
BOX = [
    [[-35.15, -59.05], [-34.30, -57.65]],  # Dock Sud / Exolgan
    [[-36.20, -57.80], [-34.30, -55.00]],  # Río exterior / aproximación
]
OUTPUT = Path("data/ais.json")
EXOLGAN_DATA = Path("data/exolgan.json")
WINDOW_SECONDS = 120
MAX_AGE_SECONDS = 30 * 60
MAX_RETRIES = 3
MAX_ATTEMPTS = MAX_RETRIES + 1


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


def normalize_name(value):
    value = unicodedata.normalize("NFKD", str(value or "")).casefold()
    return "".join(char for char in value if char.isalnum())


def is_exolgan_status(value):
    status = normalize_name(value)
    if not status or "cancel" in status:
        return False
    return (
        status in {"enviaje", "arribado", "operando", "enm uelle", "enmuelle", "enexolgan", "enrada", "enespera"}
        or "operando" in status
        or "enmuelle" in status
        or "enexolgan" in status
        or "enrada" in status
        or "enespera" in status
        or "navegando" in status
        or "entrando" in status
    )


def load_exolgan_manifest():
    data = json.loads(EXOLGAN_DATA.read_text(encoding="utf-8"))
    manifest = {}
    for vessel in data.get("vessels", []):
        if normalize_name(vessel.get("terminal")) != "exolgan":
            continue
        if not is_exolgan_status(vessel.get("status")):
            continue
        key = normalize_name(vessel.get("name"))
        if key:
            manifest[key] = {
                "schedule_name": vessel.get("name", ""),
                "schedule_status": vessel.get("status", ""),
                "eta": vessel.get("eta", ""),
            }
    if not manifest:
        raise RuntimeError("La programación no contiene buques activos de Exolgan.")
    return manifest


def position_from(event):
    metadata = event.get("MetaData") or {}
    message_type = event.get("MessageType", "Unknown")
    message = event.get("Message") or {}
    payload = message.get(message_type) or {}
    try:
        lat_value = metadata.get("Latitude", metadata.get("latitude"))
        lon_value = metadata.get("Longitude", metadata.get("longitude"))
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


async def capture_with_retries(manifest):
    all_counts = collections.Counter()
    all_positions = {}
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            current, counts = await capture()
            all_counts.update(counts)
            all_positions.update(current)
            matched = [
                vessel for vessel in current.values()
                if normalize_name(vessel.get("name")) in manifest
            ]
            if matched:
                return all_positions, all_counts, attempt, None
            if current:
                last_error = "AIS recibió posiciones, pero ninguna coincidió con la lista activa de Exolgan."
                print(f"Sin coincidencias con Exolgan en el intento {attempt}/{MAX_ATTEMPTS}.")
            else:
                last_error = "No se recibieron posiciones AIS durante la captura."
                print(f"Sin posiciones AIS en el intento {attempt}/{MAX_ATTEMPTS}.")
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:240]}".replace(KEY, "[oculto]")
            print(f"Falló AIS en el intento {attempt}/{MAX_ATTEMPTS}: {last_error}")
        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(5 * attempt)
    return all_positions, all_counts, MAX_ATTEMPTS, last_error


async def main():
    manifest = load_exolgan_manifest()
    observed, counts, attempts, last_error = await capture_with_retries(manifest)
    now = utc_now()
    matched = {}
    for mmsi, vessel in observed.items():
        schedule = manifest.get(normalize_name(vessel.get("name")))
        if schedule:
            matched[mmsi] = {**vessel, **schedule}
    previous = {}
    if OUTPUT.exists():
        try:
            old = json.loads(OUTPUT.read_text(encoding="utf-8"))
            for vessel in old.get("vessels", []):
                name_key = normalize_name(vessel.get("name"))
                if name_key not in manifest:
                    continue
                seen = parse_time(vessel.get("last_seen"), now - dt.timedelta(days=1))
                if (now - seen).total_seconds() <= MAX_AGE_SECONDS:
                    previous[str(vessel.get("mmsi"))] = {**vessel, **manifest[name_key]}
        except (OSError, json.JSONDecodeError, AttributeError):
            pass
    previous.update(matched)
    result = {
        "updated_at": now.isoformat(),
        "source": "AISStream",
        "area": "Dock Sud / Río de la Plata",
        "timezone": "America/Argentina/Buenos_Aires",
        "window_seconds": WINDOW_SECONDS,
        "attempts_made": attempts,
        "max_retries": MAX_RETRIES,
        "last_error": last_error,
        "message_count": sum(counts.values()),
        "messages": dict(counts),
        "region_vessels_in_window": len(observed),
        "unique_mmsi_in_window": len(matched),
        "coverage_confirmed": bool(matched),
        "vessels": sorted(previous.values(), key=lambda vessel: vessel["name"].casefold()),
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT.with_suffix(".json.tmp")
    temp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(OUTPUT)
    print(json.dumps({
        "updated_at": result["updated_at"],
        "attempts_made": attempts,
        "max_retries": MAX_RETRIES,
        "message_count": result["message_count"],
        "region_vessels_in_window": len(observed),
        "matched_exolgan_vessels_in_window": len(matched),
        "coverage_confirmed": result["coverage_confirmed"],
        "published_exolgan_positions": len(result["vessels"]),
        "last_error": last_error,
    }, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
