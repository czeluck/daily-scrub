#!/usr/bin/env python3
"""
fetch_feeds.py
Runs server-side (GitHub Actions) — fetches all RSS feeds, merges and
deduplicates by URL (collecting all feed labels per article), writes feeds.json.
"""

import json
import re
import html
import concurrent.futures
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

# ── FEED CATALOG ─────────────────────────────────────────────────────────────

FEEDS = [
    # WSJ (15)
    ("https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness",    "WSJ", "wsj-business"),
    ("https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",      "WSJ", "wsj-markets"),
    ("https://feeds.content.dowjones.io/public/rss/RSSWSJD",             "WSJ", "wsj-tech"),
    ("https://feeds.content.dowjones.io/public/rss/RSSWorldNews",        "WSJ", "wsj-world"),
    ("https://feeds.content.dowjones.io/public/rss/RSSUSnews",           "WSJ", "wsj-us"),
    ("https://feeds.content.dowjones.io/public/rss/socialpoliticsfeed",  "WSJ", "wsj-politics"),
    ("https://feeds.content.dowjones.io/public/rss/RSSOpinion",          "WSJ", "wsj-opinion"),
    ("https://feeds.content.dowjones.io/public/rss/rsssportsfeed",       "WSJ", "wsj-sports"),
    ("https://feeds.content.dowjones.io/public/rss/RSSStyle",            "WSJ", "wsj-style"),
    ("https://feeds.content.dowjones.io/public/rss/socialhealth",        "WSJ", "wsj-health"),
    ("https://feeds.content.dowjones.io/public/rss/RSSPersonalFinance",  "WSJ", "wsj-personalfin"),
    ("https://feeds.content.dowjones.io/public/rss/latestnewsrealestate","WSJ", "wsj-realestate"),
    ("https://feeds.content.dowjones.io/public/rss/RSSArtsCulture",      "WSJ", "wsj-artsculture"),
    ("https://feeds.content.dowjones.io/public/rss/socialeconomyfeed",   "WSJ", "wsj-economy"),
    ("https://feeds.content.dowjones.io/public/rss/RSSLifestyle",        "WSJ", "wsj-lifestyle"),

    # FT (9)
    ("https://www.ft.com/world?format=rss",            "FT", "ft-world"),
    ("https://www.ft.com/global-economy?format=rss",   "FT", "ft-economy"),
    ("https://www.ft.com/emerging-markets?format=rss", "FT", "ft-emerging"),
    ("https://www.ft.com/markets?format=rss",          "FT", "ft-markets"),
    ("https://www.ft.com/technology?format=rss",       "FT", "ft-tech"),
    ("https://www.ft.com/energy?format=rss",           "FT", "ft-energy"),
    ("https://www.ft.com/climate-capital?format=rss",  "FT", "ft-climate"),
    ("https://www.ft.com/opinion?format=rss",          "FT", "ft-opinion"),
    ("https://www.ft.com/work-careers?format=rss",     "FT", "ft-careers"),

    # Bloomberg (8)
    ("https://feeds.bloomberg.com/markets/news.rss",    "Bloomberg", "bb-markets"),
    ("https://feeds.bloomberg.com/technology/news.rss", "Bloomberg", "bb-tech"),
    ("https://feeds.bloomberg.com/politics/news.rss",   "Bloomberg", "bb-politics"),
    ("https://feeds.bloomberg.com/industries/news.rss", "Bloomberg", "bb-industries"),
    ("https://feeds.bloomberg.com/wealth/news.rss",     "Bloomberg", "bb-wealth"),
    ("https://feeds.bloomberg.com/economics/news.rss",  "Bloomberg", "bb-economics"),
    ("https://feeds.bloomberg.com/green/news.rss",      "Bloomberg", "bb-green"),
    ("https://feeds.bloomberg.com/crypto/news.rss",     "Bloomberg", "bb-crypto"),

    # NYT (16)
    ("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",            "NYT", "nyt-home"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml",               "NYT", "nyt-world"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/US.xml",                  "NYT", "nyt-us"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",            "NYT", "nyt-politics"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",            "NYT", "nyt-business"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",             "NYT", "nyt-economy"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/DealBook.xml",            "NYT", "nyt-dealbook"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/MediaandAdvertising.xml", "NYT", "nyt-media"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",          "NYT", "nyt-tech"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",             "NYT", "nyt-science"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",              "NYT", "nyt-health"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml",              "NYT", "nyt-sports"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",                "NYT", "nyt-arts"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml",             "NYT", "nyt-opinion"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml",          "NYT", "nyt-obits"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Magazine.xml",            "NYT", "nyt-magazine"),
]

HEADERS = {
    "User-Agent": "DailyScrub/2.0 (RSS reader; github-actions)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

# ── RSS PARSER ────────────────────────────────────────────────────────────────

def get_tag(block, name):
    m = re.search(rf"<{name}[^>]*>([\s\S]*?)</{name}>", block, re.IGNORECASE)
    if not m:
        return ""
    val = m.group(1)
    # Strip CDATA
    val = re.sub(r"<!\[CDATA\[([\s\S]*?)\]\]>", r"\1", val)
    return val.strip()

def get_link(block):
    """NYT and some feeds put the URL as bare text between <link> tags."""
    val = get_tag(block, "link")
    if val and val.startswith("http"):
        return val
    # Fallback: bare text after <link> (handles self-closing and whitespace variants)
    m = re.search(r"<link[^>]*/?>([^<]+)", block, re.IGNORECASE)
    if m:
        v = m.group(1).strip()
        if v.startswith("http"):
            return v
    return get_tag(block, "guid")

def parse_date(raw):
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw).astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def decode(s):
    return html.unescape(s)

def parse_feed(xml, source, feed_id):
    items = []
    blocks = re.findall(r"<item[\s\S]*?</item>", xml, re.IGNORECASE)
    for block in blocks:
        title   = decode(get_tag(block, "title"))
        link    = get_link(block)
        pub     = parse_date(get_tag(block, "pubDate") or get_tag(block, "dc:date"))
        if not title or not link or not link.startswith("http"):
            continue
        # Strip query params for dedup key (same article, different tracking params)
        dedup_key = re.sub(r"\?.*", "", link)
        items.append({
            "title":    title,
            "link":     link,
            "pub_date": pub,
            "source":   source,
            "feed":     feed_id,
            "_dedup":   dedup_key,
        })
    return items

# ── FETCH ONE FEED ────────────────────────────────────────────────────────────

def fetch_feed(args):
    url, source, feed_id = args
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        items = parse_feed(r.text, source, feed_id)
        print(f"  ✓ {feed_id:25s} {len(items)} items")
        return items
    except Exception as e:
        print(f"  ✗ {feed_id:25s} ERROR: {e}")
        return []

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"Fetching {len(FEEDS)} feeds...")

    # Fetch all feeds in parallel (10 workers — polite but fast)
    all_items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        for result in ex.map(fetch_feed, FEEDS):
            all_items.extend(result)

    print(f"\nRaw items: {len(all_items)}")

    # Deduplicate by URL (strip query params), merging feeds[] from all copies
    by_key = {}
    for item in all_items:
        key = item["_dedup"]
        if key in by_key:
            if item["feed"] not in by_key[key]["feeds"]:
                by_key[key]["feeds"].append(item["feed"])
        else:
            by_key[key] = {
                "title":    item["title"],
                "link":     item["link"],
                "pub_date": item["pub_date"],
                "source":   item["source"],
                "feeds":    [item["feed"]],
            }

    items = list(by_key.values())

    # Sort newest first (nulls last)
    items.sort(key=lambda x: x["pub_date"] or "", reverse=True)

    output = {
        "ok":         True,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "count":      len(items),
        "items":      items,
    }

    with open("feeds.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote feeds.json — {len(items)} unique articles")

if __name__ == "__main__":
    main()
