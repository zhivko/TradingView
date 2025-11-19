import redis
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUPPORTED_SYMBOLS, SUPPORTED_RESOLUTIONS, get_timeframe_seconds

r = redis.Redis(host='localhost', port=6379, db=0)

def normalize_zset(symbol: str, interval: str):
    key = f"klines_z:{symbol}:{interval}"
    interval_ms = get_timeframe_seconds(interval) * 1000

    print(f"Normalizing {key} ...")
    try:
        members = r.zrange(key, 0, -1, withscores=True)
    except Exception as e:
        print(f"Failed to read {key}: {e}")
        return

    normalized = {}
    total = len(members)
    for i, (member, score) in enumerate(members):
        if i % 10000 == 0 or i == total - 1:
            percent = (i + 1) * 100.0 / total
            print(f"Processing {i+1}/{total} entries ({percent:.2f}%)...")
        try:
            kline = json.loads(member)
            ts = int(kline[0])
            rounded_ts = (ts // interval_ms) * interval_ms
            kline[0] = rounded_ts
            normalized[rounded_ts] = json.dumps(kline, separators=(",", ":"))
        except Exception:
            continue

    if not normalized:
        print(f"No valid entries in {key}")
        return

    try:
        print("Writing normalized entries in chunks...")
        chunk_size = 10000
        items = list(normalized.items())
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i+chunk_size]
            pipe = r.pipeline()
            if i == 0:
                pipe.delete(key)
            for ts, member in chunk:
                pipe.zadd(key, {member: ts})
            print(f"Writing chunk {i//chunk_size + 1} ({i+len(chunk)}/{len(items)})...")
            pipe.execute()
        print(f"Normalized and deduplicated {len(normalized)} entries in {key}")
    except Exception as e:
        print(f"Failed to write normalized data to {key}: {e}")

if __name__ == "__main__":
    for symbol in SUPPORTED_SYMBOLS:
        for interval in SUPPORTED_RESOLUTIONS:
            normalize_zset(symbol, interval)