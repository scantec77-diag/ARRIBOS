#!/usr/bin/env python3
"""Prueba acotada de cobertura AIS en Río de la Plata / Dock Sud.
No publica ni almacena datos AIS. Requiere AISSTREAM_API_KEY en secreto.
"""
import asyncio, collections, datetime, json, os, sys
try:
    import websockets
except ImportError:
    sys.exit("Instalar dependencia: pip install websockets>=12")
KEY=os.environ.get("AISSTREAM_API_KEY")
if not KEY:
    sys.exit("Falta AISSTREAM_API_KEY. Crear una clave en aisstream.io y guardarla como secreto.")
BOX=[[[-35.15,-59.05],[-34.30,-57.65]]]
async def main():
    count=collections.Counter()
    ships=set()
    async with websockets.connect("wss://stream.aisstream.io/v0/stream",compression="deflate",open_timeout=15) as ws:
        await ws.send(json.dumps({"APIKey":KEY,"BoundingBoxes":BOX,"FilterMessageTypes":["PositionReport","StandardClassBPositionReport","ExtendedClassBPositionReport"]}))
        deadline=asyncio.get_running_loop().time()+120
        while asyncio.get_running_loop().time()<deadline:
            try: event=json.loads(await asyncio.wait_for(ws.recv(),timeout=min(15,max(0.1,deadline-asyncio.get_running_loop().time()))))
            except asyncio.TimeoutError:continue
            typ=event.get("MessageType","Unknown")
            if typ=="SubscriptionConfirmation":
                print("Suscripción aceptada:",event.get("Message"));continue
            count[typ]+=1
            m=event.get("MetaData") or {}
            if m.get("MMSI"):ships.add(str(m["MMSI"]))
    print(json.dumps({"tested_at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"seconds":120,"messages":dict(count),"unique_mmsi":len(ships),"coverage_confirmed":bool(ships)},indent=2))
asyncio.run(main())
