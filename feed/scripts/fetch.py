#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import hashlib
import re
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


# ----------------- OPML -----------------
def parse_opml(path: Path):
    """
    Collect feeds from OPML; tolerant to casing and nesting.
    On XML error, raise with context around the failing line.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as e:
        raise RuntimeError(f"Failed to read OPML at {path}: {e}")

    try:
        tree = ET.ElementTree(ET.fromstring(text))
    except ET.ParseError as pe:
        msg = str(pe)
        m = re.search(r"line (\d+), column (\d+)", msg)
        context = ""
        if m:
            ln = int(m.group(1))
            lines = text.splitlines()
            start = max(0, ln - 3)
            end = min(len(lines), ln + 2)
            snippet = "\n".join(f"{i+1:>4}: {lines[i]}" for i in range(start, end))
            context = (
                f"\n---- OPML context around line {ln} ----\n"
                f"{snippet}\n"
                f"---------------------------------------"
            )
        raise RuntimeError(
            "OPML is not well-formed: "
            f"{msg}\nHINT: Unescaped '&' is common; use '&amp;' in text/title."
            f"{context}"
        )

    feeds = []
    root = tree.getroot()

    def walk(node, group=None):
        for child in list(node):
            tag = child.tag.split("}", 1)[-1].lower()
            if tag == "outline":
                attrs = {k.lower(): v for k, v in child.attrib.items()}
                xml_url = attrs.get("xmlurl") or attrs.get("url") or attrs.get("htmlurl")
                text    = attrs.get("text") or attrs.get("title") or ""
                if xml_url:
                    feeds.append({"title": text, "url": xml_url, "category": group or ""})
                # If this outline is a folder, pass its name as category to children
                walk(child, group=text or group)
            else:
                walk(child, group)
    walk(root, None)
    return feeds


# ----------------- Rules -----------------
def load_rules(path: Path):
    """
    Load YAML rules with strong defaults and normalisation so
    empty keys (null) don't crash the script.
    """
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
    if not path.exists():
        return defaults

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    def _as_list(x):
        if isinstance(x, list):
            return x
        return [] if x in (None, "", False) else [x]

    def _as_dict(x):
        return x if isinstance(x, dict) else {}

    out = {**defaults, **data}
    out["include_keywords"]   = _as_list(out.get("include_keywords"))
    out["blocklist_keywords"] = _as_list(out.get("blocklist_keywords"))
    out["include_sources"]    = _as_list(out.get("include_sources"))
    out["exclude_sources"]    = _as_list(out.get("exclude_sources"))
    out["include_authors"]    = _as_list(out.get("include_authors"))
    out["exclude_authors"]    = _as_list(out.get("exclude_authors"))
    out["include_tags"]       = _as_list(out.get("include_tags"))
    out["exclude_tags"]       = _as_list(out.get("exclude_tags"))
    out["max_per_source"]     = _as_dict(out.get("max_per_source"))
    out["pin"]                = _as_list(out.get("pin"))

    for k, dflt in [("min_title_length", 0), ("max_items", 500), ("max_age_days", 36500)]:
        try:
            out[k] = int(out.get(k, dflt) or dflt)
        except Exception:
            out[k] = dflt

    return out


# ----------------- Helpers -----------------
def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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
    # Look in common media fields
    for key in ("media_thumbnail", "media_content"):
        arr = entry.get(key)
        if isinstance(arr, list) and arr:
            url = arr[0].get("url")
            if url:
                return url
    # Fallback to image enclosures
    for enc in (entry.get("enclosures") or []):
        if enc.get("type", "").startswith("image/") and enc.get("href"):
            return enc["href"]
    return ""


def collect_tags(entry):
    tags = []
    for t in (entry.get("tags") or []):
        term = t.get("term") or t.get("label")
        if term:
            tags.append(term)
    return tags


def matches_any(text: str, needles):
    if not needles:
        return False
    t = (text or "").casefold()
    for n in needles:
        if not n:
            continue
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
    src_hit = (item["source"] or dom or "").casefold()
    if rules["include_sources"]:
        if not any(str(s).casefold() in (src_hit, dom) for s in rules["include_sources"]):
            return False
    for s in rules["exclude_sources"]:
        s = str(s).casefold()
        if s and (s in src_hit or s == dom):
            return False

    # Author allow/deny
    auth = (item.get("author") or "").casefold()
    if rules["include_authors"] and not any(a and str(a).casefold() in auth for a in rules["include_authors"]):
        return False
    if any(a and str(a).casefold() in auth for a in rules["exclude_authors"]):
        return False

    # Tags allow/deny
    tags = [str(t).casefold() for t in (item.get("tags") or [])]
    if rules["include_tags"] and not any(str(x).casefold() in tags for x in rules["include_tags"]):
        return False
    if any(str(x).casefold() in tags for x in rules["exclude_tags"]):
        return False

    # Title length
    if len(item["title"]) < int(rules["min_title_length"]):
        return False

    return True


# ----------------- Fetch & normalise -----------------
def fetch_entries(url: str):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        return feedparser.parse(r.content)
    except Exception as e:
        print(f"[error] Fetch {url}: {e}")
        return feedparser.parse(b"")


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


# ----------------- Main -----------------
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
        caps_map = rules.get("max_per_source") or {}
        try:
            cap = int(caps_map.get(cap_key, 10**9))
        except Exception:
            cap = 10**9
        per_source_count.setdefault(cap_key, 0)

        if getattr(d, "bozo", 0):
            print(f"[warn] Parse issue on {title or url}: {getattr(d, 'bozo_exception', '')}")

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
        seen.add(key)
        dedup.append(it)

    # Pin items
    for p in (rules.get("pin") or []):
        link = p.get("url", "")
        title = p.get("title", "")
        note = p.get("note", "")
        uid = hashlib.sha1((link or title).encode("utf-8")).hexdigest()
        dedup.insert(0, {
            "id": uid, "title": title, "link": link, "summary": note,
            "isoDate": now_iso(), "source": "Pinned", "category": "", "author": "",
            "tags": [], "image": "", "pinned": True,
        })

    # Cap history
    try:
        max_items = int(rules.get("max_items", 500))
    except Exception:
        max_items = 500
    dedup = dedup[:max_items]

    OUT.write_text(json.dumps(dedup, indent=2, ensure_ascii=False))
    print(f"[ok] Wrote {len(dedup)} items -> {OUT}")


if __name__ == "__main__":
    main()