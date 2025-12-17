
import os
import json
import csv
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

FEEDS = {
    "incidents": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX2luY2lkZW50c19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=cDk2OFRBSExHd2lOMGJsa2l1eWs5bnNOamxiQnVPVVQ0c3I3YVhqa1ZURT0=&h=3326e40ea2974feaa7cd9afc39ffdda1&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "roadworks": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX3JvYWR3b3Jrc19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=aktjeThvUms5dFB6MjY2TisxR1oxNkgvYkFuR2JGN2g1cjB4K0ZEb2hxTT0=&h=3326e40ea2974feaa7cd9afc39ffdda1&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "events": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX2V2ZW50c19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=eDAvS0dXT2VjdzYxNytCeU81N1A4aFhMRWF0cXU1a0M3cTBMWXliU3R2Yz0=&h=3326e40ea2974feaa7cd9afc39ffdda1&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "news": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX25ld3NfcnNzLnhtbA==&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=aUloNHU2RHNDbkZtMy84R2NXMUo4UFNQbGphM204ZnFYM0ZGbklmQ1Ficz0=&h=3326e40ea2974feaa7cd9afc39ffdda1&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
}

DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "trafficwatch.csv") 
JSON_PATH = os.path.join(DATA_DIR, "trafficwatch.json")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (GitHub Actions trafficwatch logger)",
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def parse_rss(xml_bytes: bytes, feed_type: str):
    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else []

    out = []
    for it in items:
        def txt(tag):
            el = it.find(tag)
            return (el.text or "").strip() if el is not None else ""

        title = txt("title")
        link = txt("link")
        guid = txt("guid") or link or title  # stable-ish fallback

        out.append({
            "feed_type": feed_type,
            "guid": guid,
            "title": title,
            "pub_date": txt("pubDate"),
            "description": txt("description"),
            "link": link,
        })
    return out


def make_key(row: dict) -> str:
    return f"{row.get('feed_type','')}::{row.get('guid','')}"


def load_existing_history() -> list[dict]:
    # Prefer JSON if present (richer), else fall back to CSV
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    if os.path.exists(CSV_PATH):
        try:
            with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception:
            return []

    return []


def normalise_row_types(row: dict) -> dict:
    # If loaded from CSV, everything is string. Fix types for known fields.
    r = dict(row)
    if "seen_count" in r:
        try:
            r["seen_count"] = int(r["seen_count"])
        except Exception:
            r["seen_count"] = 0
    return r


def write_json(rows: list[dict]) -> None:
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def write_csv(rows: list[dict]) -> None:
    # Stable column order for Power BI
    fieldnames = [
        "feed_type",
        "guid",
        "title",
        "pub_date",
        "description",
        "link",
        "first_seen_utc",
        "last_seen_utc",
        "seen_count",
    ]
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    run_time = now_utc_iso()

    # Load existing history and index it
    existing = [normalise_row_types(r) for r in load_existing_history()]
    history_by_key: dict[str, dict] = {make_key(r): r for r in existing if make_key(r) != "::"}

    # Fetch current items across feeds
    current_items = []
    for feed_type, url in FEEDS.items():
        xml_bytes = fetch(url)
        current_items.extend(parse_rss(xml_bytes, feed_type))

    # Mark what we saw this run
    seen_this_run = set()

    for item in current_items:
        k = make_key(item)
        if not item.get("guid"):
            continue

        seen_this_run.add(k)

        if k in history_by_key:
            # Update existing record
            rec = history_by_key[k]
            rec["last_seen_utc"] = run_time
            rec["seen_count"] = int(rec.get("seen_count", 0)) + 1

            # Optional: refresh mutable fields if they changed
            # (titles/descriptions sometimes get edited)
            rec["title"] = item.get("title", rec.get("title", ""))
            rec["pub_date"] = item.get("pub_date", rec.get("pub_date", ""))
            rec["description"] = item.get("description", rec.get("description", ""))
            rec["link"] = item.get("link", rec.get("link", ""))
        else:
            # New record
            history_by_key[k] = {
                **item,
                "first_seen_utc": run_time,
                "last_seen_utc": run_time,
                "seen_count": 1,
            }

    # Convert back to list and sort (newest first by first_seen_utc)
    history = list(history_by_key.values())
    history.sort(key=lambda r: (r.get("first_seen_utc", ""), r.get("pub_date", "")), reverse=True)

    # Write both outputs
    write_json(history)
    write_csv(history)


if __name__ == "__main__":
    main()
