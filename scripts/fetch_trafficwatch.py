
import json
import csv
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

FEEDS = {
    "incidents": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX2luY2lkZW50c19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=cDk2OFRBSExHd2lOMGJsa2l1eWs5bnNOamxiQnVPVVQ0c3I3YVhqa1ZURT0=&h=ceec728b4f3a45c499eec6aefc7284b4&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "roadworks": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX3JvYWR3b3Jrc19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=aktjeThvUms5dFB6MjY2TisxR1oxNkgvYkFuR2JGN2g1cjB4K0ZEb2hxTT0=&h=ceec728b4f3a45c499eec6aefc7284b4&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "events": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX2V2ZW50c19yc3MueG1s&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=eDAvS0dXT2VjdzYxNytCeU81N1A4aFhMRWF0cXU1a0M3cTBMWXliU3R2Yz0=&h=ceec728b4f3a45c499eec6aefc7284b4&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
    "news": "https://eu-west-1.protection.sophos.com?d=trafficwatchni.com&u=aHR0cHM6Ly9yc3MudHJhZmZpY3dhdGNobmkuY29tL3RyYWZmaWN3YXRjaG5pX25ld3NfcnNzLnhtbA==&i=NjU1NTQ1ZDA2MjBmMmMyMzA0ZGU2Y2Y1&t=aUloNHU2RHNDbkZtMy84R2NXMUo4UFNQbGphM204ZnFYM0ZGbklmQ1Ficz0=&h=ceec728b4f3a45c499eec6aefc7284b4&s=AVNPUEhUT0NFTkNSWVBUSVbXe3g3GHVCxGq3v6CjsfAx0qRWlfqpAu4MMzVkv7QgyA",
}

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
    fetched_at_utc = datetime.now(timezone.utc).isoformat()

    for it in items:
        def txt(tag):
            el = it.find(tag)
            return (el.text or "").strip() if el is not None else ""

        out.append({
            "fetched_at_utc": fetched_at_utc,
            "feed_type": feed_type,
            "guid": txt("guid") or txt("link") or txt("title"),
            "title": txt("title"),
            "pub_date": txt("pubDate"),
            "description": txt("description"),
            "link": txt("link"),
        })
    return out

def main():
    all_rows = []
    for feed_type, url in FEEDS.items():
        xml_bytes = fetch(url)
        all_rows.extend(parse_rss(xml_bytes, feed_type))

    # Write JSON
    with open("data/trafficwatch.json", "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    # Write CSV
    fieldnames = ["fetched_at_utc","feed_type","guid","title","pub_date","description","link"]
    with open("data/trafficwatch.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

if __name__ == "__main__":
    main()
