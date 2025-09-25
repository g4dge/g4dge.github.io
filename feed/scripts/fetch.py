#!/usr/bin/env python3
import json, time, hashlib, re
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import feedparser
import yaml

# ----- Paths (repo root → /feed subtree) -----
ROOT = Path(__file__).resolve().parents[2]     # from feed/scripts/* -> repo root
BASE = ROOT / "feed"

OPML  = BASE / "config" / "sources.opml"
RULES = BASE / "config" / "rules.yml"
OUT   = BASE / "data"   / "items.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

UA = "Rob-AntiFeed/1.0 (+https://g4dge.github.io/feed) Python-Requests"
TIMEOUT = 20

def parse_opml(path: Path):
    """Collect feeds from OPML, tolerating different attribute casings and layouts."""
    feeds = []
    tree = ET.parse(path)
    root = tree.getroot()

    def walk(node):
        for child in list(node):
            # Strip any XML namespace
            tag = child.tag.split('}', 1)[-1].lower()
            if tag == "outline":
                attrs = {k.lower(): v for k, v in child.attrib.items()}
                # Accept common variants
                xml_url = (
                    attrs.get("xmlurl")
                    or attrs.get("url")
                    or attrs.get("htmlurl")
                )
                text = attrs.get("text") or attrs.get("title") or ""
                if xml_url:
                    feeds.append({"title": text, "url": xml_url})
                # Recurse into nested outlines
                walk(child)
            else:
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
    for kw in (rules.get("blocklist_keywords") or []):
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
    uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest()
    return {
        "id": uid,
        "title": (entry.get("title") or "").strip(),
        "link": link,
        "summary": (entry.get("summary") or "").strip(),
        "isoDate": ts,
        "source": feed_title or "",
    }

def fetch_entries(url: str):
    """Fetch via requests (custom UA) then parse with feedparser."""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        return feedparser.parse(r.content)
    except Exception as e:
        print(f"[error] Fetch {url}: {e}")
        return feedparser.parse(b"")

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
    total_raw = 0
    for f in feeds:
        url, title = f["url"], (f["title"] or "")
        d = fetch_entries(url)
        raw = len(d.entries or [])
        total_raw += raw
        kept = 0
        if getattr(d, "bozo", 0):
            print(f"[warn] Parse issue on {title} ({url}): {getattr(d, 'bozo_exception', '')}")
        for e in d.entries or []:
            if keep_item(e, rules):
                items.append(norm_item(e, title))
                kept += 1
        print(f"[info] {title or url}: raw={raw} kept={kept}")

    print(f"[info] Total: raw={total_raw} kept={len(items)}")

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
