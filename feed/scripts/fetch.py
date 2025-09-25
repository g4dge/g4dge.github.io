import json, time, hashlib, re, os
from pathlib import Path
import feedparser
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "feed"

OPML  = BASE / "config" / "sources.opml"
RULES = BASE / "config" / "rules.yml"
OUT   = BASE / "data"   / "items.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

def parse_opml(path):
    tree = ET.parse(path)
    feeds = []
    for node in tree.iter():
        if node.tag == "outline" and node.attrib.get("type") == "rss":
            feeds.append({
                "title": node.attrib.get("text") or node.attrib.get("title"),
                "url": node.attrib["xmlUrl"]
            })
    return feeds

def load_rules(path):
    if not path.exists(): return {"blocklist_keywords": [], "min_title_length": 0, "max_items": 500, "pin": []}
    return yaml.safe_load(path.read_text())

def keep_item(entry, rules):
    title = (entry.get("title") or "").strip()
    if len(title) < rules.get("min_title_length", 0): return False
    text = " ".join([title, entry.get("summary","")]).lower()
    for kw in rules.get("blocklist_keywords", []):
        if re.search(rf"\b{re.escape(kw.lower())}\b", text):
            return False
    return True

def norm_item(entry, feed_title):
    # isoDate: fallback order
    ts = None
    for k in ["published_parsed", "updated_parsed"]:
        if entry.get(k):
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", entry[k])
            break
    if not ts:
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    link = (entry.get("link") or "").strip()
    # Build a stable unique id from link or title+timestamp
    raw_uid = link if link else f"{entry.get('title','')}{ts}"
    uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest()

    return {
        "id": uid,
        "title": (entry.get("title") or "").strip(),
        "link": link,
        "summary": (entry.get("summary") or "").strip(),
        "isoDate": ts,
        "source": feed_title,
    }

def main():
    feeds = parse_opml(OPML)
    rules = load_rules(RULES)
    items = []
    for f in feeds:
        d = feedparser.parse(f["url"])
        for e in d.entries:
            if keep_item(e, rules):
                items.append(norm_item(e, f["title"] or ""))
    # de-dup by link, newest first
    seen = set(); dedup=[]
    for it in sorted(items, key=lambda x: x["isoDate"], reverse=True):
        if it["link"] in seen: continue
        seen.add(it["link"]); dedup.append(it)
        print(f"Wrote {len(dedup)} items to {OUT}")

    # pin manual items to the top (if provided)
    for p in rules.get("pin", []):
        dedup.insert(0, {
            "id": hashlib.sha1(p["url"].encode()).hexdigest(),
            "title": p["title"],
            "link": p["url"],
            "summary": p.get("note",""),
            "isoDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "Pinned"
        })

    # cap history
    max_items = int(rules.get("max_items", 500))
    dedup = dedup[:max_items]

    OUT.write_text(json.dumps(dedup, indent=2))
    print(f"Wrote {len(dedup)} items to {OUT}")

if __name__ == "__main__":
    main()
