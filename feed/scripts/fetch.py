#!/usr/bin/env python3
import json, time, hashlib, re
from pathlib import Path
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

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

def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def parse_opml(path: Path):
    """Collect feeds; retain the parent group as 'category'."""
    feeds = []
    tree = ET.parse(path)
    root = tree.getroot()

    def walk(node, group=None):
        for child in list(node):
            tag = child.tag.split('}', 1)[-1].lower()
            if tag == "outline":
                attrs = {k.lower(): v for k, v in child.attrib.items()}
                xml_url = attrs.get("xmlurl") or attrs.get("url") or attrs.get("htmlurl")
                text    = attrs.get("text") or attrs.get("title") or ""
                if xml_url:
                    feeds.append({"title": text, "url": xml_url, "category": group or ""})
                # Recurse; if this is a folder, pass its name as group
                walk(child, group=text or group)
            else:
                walk(child, group)
    walk(root, None)
    return feeds

def load_rules(path: Path):
    defaults = {
        "min_title_length": 0,
        "max_items": 500,
        "max_age_days": 36500,
        "include_keywords": [],
        "blocklist_keywords": [],
        "include_sources": [],
        "exclude_sources": [],
        "include_authors": [],
        "exclude_authors": [],
        "include_tags": [],
        "exclude_tags": [],
        "max_per_source": {},
        "pin": [],
    }
    if not path.exists(): return defaults
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    defaults.update(data)
    return defaults

def to_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""

def _iso_from_entry(entry):
    for k in ("published_parsed", "updated_parsed"):
        t = entry.get(k)
        if t:
            try:
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)
            except Exception:
                pass
    return now_iso()

def _age_days(iso_ts: str) -> float:
    try:
        t_struct = time.strptime(iso_ts, "%Y-%m-%dT%H:%M:%SZ")
        return (time.mktime(time.gmtime()) - time.mktime(t_struct)) / 86400.0
    except Exception:
        return 0.0

def extract_first_image(entry):
    # media_thumbnail / media_content / enclosure
    for key in ("media_thumbnail", "media_content"):
        arr = entry.get(key)
        if isinstance(arr, list) and arr:
            url = arr[0].get("url")
            if url: return url
    for enc in entry.get("enclosures", []) or []:
        if enc.get("type","").startswith("image/") and enc.get("href"):
            return enc["href"]
    return ""

def collect_tags(entry):
    tags = []
    for t in entry.get("tags", []) or []:
        term = t.get("term") or t.get("label")
        if term: tags.append(term)
    return tags

def norm_item(entry, feed_title, category):
    ts   = _iso_from_entry(entry)
    link = (entry.get("link") or "").strip()
    raw_uid = link if link else f"{entry.get('title','')}{ts}"
    uid  = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest()
    return {
        "id": uid,
        "title": (entry.get("title") or "").strip(),
        "link": link,
        "summary": (entry.get("summary") or "").strip(),
        "isoDate": ts,
        "source": feed_title or "",
        "category": category or "",
        "author": (entry.get("author") or "").strip(),
        "tags": collect_tags(entry),
        "image": extract_first_image(entry) or "",
        "pinned": False,
    }

def fetch_entries(url: str):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        return feedparser.parse(r.content)
    except Exception as e:
        print(f"[error] Fetch {url}: {e}")
        return feedparser.parse(b"")

def matches_any(text: str, needles):
    if not needles: return False
    t = (text or "").casefold()
    for n in needles:
        if not n: continue
        if str(n).casefold() in t:
            return True
    return False

def allowed_by_lists(item, rules):
    # Keywords
    text = f"{item['title']} {item['summary']}"
    if rules["include_keywords"] and not matches_any(text, rules["include_keywords"]):
        return False
    if matches_any(text, rules["blocklist_keywords"]):
        return False

    # Age
    if _age_days(item["isoDate"]) > float(rules["max_age_days"]):
        return False

    # Source allow/deny (by title or domain)
    dom = to_domain(item["link"])
    src_hit = item["source"] or dom
    if rules["include_sources"]:
        if all(s.casefold() not in (src_hit.casefold(), dom) for s in rules["include_sources"]):
            return False
    for s in rules["exclude_sources"]:
        if s and (s.casefold() in src_hit.casefold() or s.casefold() == dom):
            return False

    # Author allow/deny
    auth = (item.get("author") or "").casefold()
    if rules["include_authors"] and not any(a and a.casefold() in auth for a in rules["include_authors"]):
        return False
    if any(a and a.casefold() in auth for a in rules["exclude_authors"]):
        return False

    # Tags allow/deny
    tags = [t.casefold() for t in (item.get("tags") or [])]
    if rules["include_tags"] and not any(x and str(x).casefold() in tags for x in rules["include_tags"]):
        return False
    if any(x and str(x).casefold() in tags for x in rules["exclude_tags"]):
        return False

    # Title length
    if len(item["title"]) < int(rules["min_title_length"]):
        return False

    return True

def main():
    rules  = load_rules(RULES)
    feeds  = parse_opml(OPML)
    print(f"[info] OPML: {len(feeds)} feeds from {OPML}")

    items = []
    per_source_count = {}

    for f in feeds:
        url, title, category = f["url"], (f["title"] or ""), (f.get("category") or "")
        d = fetch_entries(url)
        raw = len(d.entries or [])
        kept = 0

        # per-source cap (match by title or domain)
        cap_key = title or to_domain(url)
        cap = int(rules.get("max_per_source", {}).get(cap_key, 10**9))
        per_source_count.setdefault(cap_key, 0)

        for e in d.entries or []:
            it = norm_item(e, title, category)
            if not allowed_by_lists(it, rules):
                continue
            if per_source_count[cap_key] >= cap:
                continue
            items.append(it)
            per_source_count[cap_key] += 1
            kept += 1

        print(f"[info] {title or url}: raw={raw} kept={kept} cap={cap} sofar={per_source_count[cap_key]}")

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
            "isoDate": now_iso(), "source": "Pinned", "category": "", "author": "",
            "tags": [], "image": "", "pinned": True,
        })

    # Cap
    dedup = dedup[: int(rules.get("max_items", 500))]

    OUT.write_text(json.dumps(dedup, indent=2, ensure_ascii=False))
    print(f"[ok] Wrote {len(dedup)} items -> {OUT}")

if __name__ == "__main__":
    main()