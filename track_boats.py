#!/usr/bin/env python3
"""Ascolta AISstream per qualche decina di secondi e aggiorna boats.geojson
con l'ultima posizione nota di ogni barca elencata in boats.json.
Versione con diagnostica: stampa cosa succede e, se nessuna barca arriva,
verifica se il servizio manda dati in generale."""
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

# Zona di prova (Mar Ligure / Toscana) usata solo per la diagnostica
TEST_BOX = [[[42.0, 8.0], [44.5, 11.0]]]


def log(*a):
    print(*a, flush=True)


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


async def listen(mmsis, state, stats, boxes, use_filter):
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": boxes,
        "FilterMessageTypes": [
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        ],
    }
    if use_filter:
        sub["FiltersShipMMSI"] = list(mmsis)
    try:
        async with websockets.connect(URL, open_timeout=25) as ws:
            log("Connesso ad AISstream, sottoscrizione inviata")
            await ws.send(json.dumps(sub))
            while True:
                raw = await ws.recv()
                msg = json.loads(raw)
                if "error" in msg:
                    log("ERRORE da AISstream:", msg["error"])
                    stats["error"] = msg["error"]
                    return
                stats["total"] += 1
                meta = msg.get("MetaData", {})
                mmsi = str(meta.get("MMSI", ""))
                if not use_filter:
                    # fase di prova: stampiamo solo qualche esempio
                    if stats["total"] <= 3:
                        log("  esempio ricevuto: MMSI", mmsi, meta.get("ShipName", "").strip())
                    continue
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
                stats["matched"] += 1
                log("Aggiornata", mmsi, mmsis[mmsi])
    except websockets.exceptions.ConnectionClosed as e:
        log("Connessione chiusa dal servizio:", e)
        stats["closed"] = str(e)
    except Exception as e:
        log("ERRORE di connessione:", type(e).__name__, e)
        stats["conn_error"] = repr(e)


async def run_phase(mmsis, state, stats, boxes, use_filter, seconds):
    try:
        await asyncio.wait_for(listen(mmsis, state, stats, boxes, use_filter), timeout=seconds)
    except asyncio.TimeoutError:
        pass  # normale: tempo di ascolto finito


async def main():
    if not API_KEY:
        sys.exit("Manca la variabile AISSTREAM_KEY")
    log("Lunghezza chiave:", len(API_KEY), "caratteri")
    mmsis = load_json(BOATS_FILE, {})
    state = load_json(STATE_FILE, {})
    log("Barche in elenco:", ", ".join(f"{k} ({v})" for k, v in mmsis.items()))

    # Fase 1: le tue barche (tutto il mondo, filtrate per MMSI)
    stats = {"total": 0, "matched": 0}
    await run_phase(mmsis, state, stats, [[[-90, -180], [90, 180]]], True, LISTEN_SECONDS)
    log(f"Fase 1 finita: {stats['matched']} messaggi delle tue barche")

    # Fase 2 (solo diagnostica): se non e' arrivato nulla, controlliamo il servizio
    if stats["matched"] == 0:
        log("Nessuna tua barca ricevuta. Controllo se il servizio manda dati in generale...")
        test = {"total": 0, "matched": 0}
        await run_phase(mmsis, {}, test, TEST_BOX, False, 30)
        if test.get("error"):
            log("DIAGNOSI: errore del servizio ->", test["error"], "(probabile chiave non valida)")
        elif test["total"] > 0:
            log(f"DIAGNOSI: il servizio funziona ({test['total']} messaggi in 30 s nel Mar Ligure). "
                "Il problema e' che le tue barche non risultano trasmettere o non hanno copertura "
                "(oppure MMSI sbagliato).")
        else:
            log("DIAGNOSI: nemmeno la zona di prova ha dato messaggi: problema di chiave, "
                "connessione o servizio.")

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
    log(f"{len(features)} barche su {len(mmsis)} nel file {OUT_FILE}")


asyncio.run(main())
