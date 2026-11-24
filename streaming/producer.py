#!/usr/bin/env python3
"""Simula CFDI nuevos publicándolos en Kafka (tópico cfdi-nuevos).
Re-emite filas de los CSV generados con UUID y fecha nuevos, a --rate mensajes/seg."""
import argparse, glob, json, time, uuid
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="cfdi-nuevos")
    ap.add_argument("--source", default="data/raw/cfdi/*.csv")
    ap.add_argument("--rate", type=float, default=50, help="mensajes por segundo")
    ap.add_argument("--limit", type=int, default=0, help="0 = sin límite")
    a = ap.parse_args()

    p = Producer({"bootstrap.servers": a.bootstrap})
    sent, nxt = 0, time.time()
    for f in sorted(glob.glob(a.source)):
        for chunk in pd.read_csv(f, chunksize=10_000):
            for rec in chunk.to_dict("records"):
                rec["uuid"] = str(uuid.uuid4())
                rec["fecha"] = datetime.now().isoformat(timespec="seconds")
                p.produce(a.topic, key=str(rec["rfc_emisor"]), value=json.dumps(rec))
                p.poll(0)
                sent += 1
                if a.limit and sent >= a.limit:
                    p.flush(); print(f"{sent} mensajes enviados"); return
                nxt += 1 / a.rate
                time.sleep(max(0, nxt - time.time()))
                if sent % 500 == 0:
                    print(f"{sent} enviados", flush=True)
    p.flush()


if __name__ == "__main__":
    main()
