#!/usr/bin/env python3
import json, time, hashlib, re
from pathlib import Path
import xml.etree.ElementTree as ET

import feedparser
import yaml

# ----- Paths (repo root → /feed subtree) -----
ROOT = Path(__file__).resolve().parents[2]     # from feed/scripts/* -> repo root
BASE = ROOT / "feed"

OPML  = BASE / "config" / "sources.opml"
RULES = BASE / "config" / "rules.yml"
OUT   = BASE / "data"   / "items.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

def parse_opml(path: Path):
    """Return a list of {'title','url'} from OPML (supports nested outlines)."""
    feeds = []
    tree = ET.parse(path)
    root = tree.getroot()

    def walk(node):
        for child in node:
            if child.tag.lower() == "outline":
                xml_url = child.attrib.get("xmlUrl") or child.attrib.get("xmlurl")
                typ = (child.attrib.get("type") or "").lower()
                text = child.attrib.get("text") or child.attrib.get("title") or ""
                if xml_url and (typ in ("rss", "atom", "")):  # some OPML omit 'type'
                    feeds.append({"title": text, "url": xml_url})
                walk(child)

    walk(root)
    return feeds

def load_rules(path: Path):
    if not path.exists(): return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}

def keep_item(entry, rules):
    title = (entry.get("title") or "").strip()
    if len(title) < int(rules.get("min_title_length", 0)):
        return False
    text = f"{title} {(entry.get('summary') or '')}".casefold()
    for kw in rules.get("blocklist_keywords", []) or []:
        if re.search(rf"\b{re.escape(str(kw).casefold())}\b", text):
            return False
    return True

def _iso_from_entry(entry):
    for k in ("published_parsed", "updated_parsed"):
        t = entry.get(k)
        if t:
            try:
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)
            except Exception:
                pass
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def norm_item(entry, feed_title):
    ts = _iso_from_entry(entry)
    link = (entry.get("link") or "").strip()
    raw_uid = link if link else f"{entry.get('title','')}{ts}"
    uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest()   # <-- FIX

    return {
        "id": uid,
        "title": (entry.get("title") or "").strip(),
        "link": link,
        "summary": (entry.get("summary") or "").strip(),
        "isoDate": ts,
        "source": feed_title or "",
    }

def main():
    rules = {
        "blocklist_keywords": [],
        "min_title_length": 0,
        "max_items": 500,
        "pin": [],
    }
    rules.update(load_rules(RULES))

    feeds = parse_opml(OPML)
    print(f"[info] OPML: {len(feeds)} feeds from {OPML}")

    items = []
    total = 0
    for f in feeds:
        url = f["url"]
        d = feedparser.parse(url)
        total += len(d.entries or [])
        if getattr(d, "bozo", 0):
            print(f"[warn] Parse issue on {url}: {getattr(d, 'bozo_exception', '')}")
        for e in d.entries or []:
            if keep_item(e, rules):
                items.append(norm_item(e, f["title"] or ""))

    print(f"[info] Pulled {total} entries, kept {len(items)} after filters")

    # De-dup newest first; prefer link, fallback to id
    seen, dedup = set(), []
    for it in sorted(items, key=lambda x: x["isoDate"], reverse=True):
        key = it["link"] or it["id"]
        if key in seen: 
            continue
        seen.add(key); dedup.append(it)

    # Pin items
    for p in (rules.get("pin") or []):
        link = p.get("url", ""); title = p.get("title", ""); note = p.get("note", "")
        uid = hashlib.sha1((link or title).encode("utf-8")).hexdigest()
        dedup.insert(0, {
            "id": uid, "title": title, "link": link, "summary": note,
            "isoDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "Pinned",
        })

    # Cap
    dedup = dedup[: int(rules.get("max_items", 500))]

    OUT.write_text(json.dumps(dedup, indent=2, ensure_ascii=False))
    print(f"[ok] Wrote {len(dedup)} items -> {OUT}")

if __name__ == "__main__":
    main()
