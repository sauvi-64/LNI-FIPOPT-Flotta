#!/usr/bin/env python3
"""Ascolta AISstream per qualche decina di secondi e aggiorna boats.geojson
con l'ultima posizione nota di ogni barca elencata in boats.json."""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import websockets

API_KEY = os.environ.get("AISSTREAM_KEY")
LISTEN_SECONDS = int(os.environ.get("LISTEN_SECONDS", "90"))
BOATS_FILE = "boats.json"        # {"247123456": "Nome barca", ...}
STATE_FILE = "state.json"        # ultime posizioni note (persistenti tra le esecuzioni)
OUT_FILE = "boats.geojson"       # file da dare a Windy
URL = "wss://stream.aisstream.io/v0/stream"


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


async def listen(mmsis, state):
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],  # tutto il mondo, filtrato per MMSI
        "FiltersShipMMSI": list(mmsis),
        "FilterMessageTypes": [
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        ],
    }
    async with websockets.connect(URL) as ws:
        await ws.send(json.dumps(sub))
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if "error" in msg:
                print("Errore da AISstream:", msg["error"], file=sys.stderr)
                return
            meta = msg.get("MetaData", {})
            mmsi = str(meta.get("MMSI", ""))
            if mmsi not in mmsis:
                continue
            body = next(iter(msg.get("Message", {}).values()), {})
            state[mmsi] = {
                "lat": meta.get("latitude"),
                "lon": meta.get("longitude"),
                "sog": body.get("Sog"),
                "cog": body.get("Cog"),
                "time_utc": meta.get("time_utc"),
                "seen_by_script": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            print("Aggiornata", mmsi, mmsis[mmsi])


async def main():
    if not API_KEY:
        sys.exit("Manca la variabile AISSTREAM_KEY")
    mmsis = load_json(BOATS_FILE, {})
    state = load_json(STATE_FILE, {})
    try:
        await asyncio.wait_for(listen(mmsis, state), timeout=LISTEN_SECONDS)
    except asyncio.TimeoutError:
        pass  # normale: abbiamo ascoltato per il tempo previsto

    features = []
    for mmsi, name in mmsis.items():
        p = state.get(mmsi)
        if not p or p["lat"] is None or p["lon"] is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]},
            "properties": {
                "name": name,
                "mmsi": mmsi,
                "speed_kn": p["sog"],
                "course": p["cog"],
                "last_position": p["time_utc"],
            },
        })

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, indent=1)
    print(f"{len(features)} barche su {len(mmsis)} nel file {OUT_FILE}")


asyncio.run(main())
