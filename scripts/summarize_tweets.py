#!/usr/bin/env python3
"""Summarize tweets collected in MongoDB and write a text summary file.

Outputs:
- Total number of tweets
- Time span (if available)
- Top hashtags
- Top mentioned users
- Top words (non-stopwords)
- Language distribution (if available)
- Representative tweets (top by engagement or length)

Environment variables:
- MONGODB_CONNECT: MongoDB connection string

Usage:
- python scripts/summarize_tweets.py --limit 100 --out artifacts/tweet_summary.txt --top 5
"""

import os
import re
import sys
import logging
from collections import Counter
from datetime import datetime
from typing import List

import argparse
import nltk
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_RE = re.compile(r"@([A-Za-z0-9_]{1,15})")


def get_env_var(name: str) -> str:
    v = os.getenv(name)
    if not v:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return v


def ensure_nltk():
    try:
        nltk.data.find("tokenizers/punkt")
    except Exception:
        logger.info("Downloading punkt tokenizer")
        nltk.download("punkt")
    try:
        nltk.data.find("corpora/stopwords")
    except Exception:
        logger.info("Downloading stopwords")
        nltk.download("stopwords")


def summarize_tweets(mongo_uri: str, limit: int = 0, top: int = 5, out_path: str = "artifacts/tweet_summary.txt") -> str:
    client = MongoClient(mongo_uri)
    db = client["demo"]
    coll = db["tweet_collection"]

    projection = {"text": 1, "created_at": 1, "lang": 1, "public_metrics": 1, "entities": 1, "author_id": 1}
    cursor = coll.find({}, projection)
    if limit and limit > 0:
        cursor = cursor.limit(limit)
    tweets = list(cursor)

    if not tweets:
        logger.info("No tweets found in demo.tweet_collection")
        return ""

    ensure_nltk()
    stopwords = set(nltk.corpus.stopwords.words("english"))

    total = len(tweets)
    dates = []
    hashtags = Counter()
    mentions = Counter()
    words = Counter()
    langs = Counter()
    rep_candidates = []

    for t in tweets:
        text = (t.get("text") or "")
        # created_at may be a string or missing
        created = t.get("created_at")
        if created:
            try:
                # try ISO format
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                dates.append(dt)
            except Exception:
                pass

        # structured entities if present
        ent = t.get("entities") or {}
        ents_mentions = []
        try:
            for m in ent.get("mentions", []) or []:
                uname = m.get("username") or m.get("screen_name")
                if uname:
                    mentions[uname.lower()] += 1
                    ents_mentions.append(uname.lower())
        except Exception:
            pass

        # regex fallback for mentions
        for m in MENTION_RE.findall(text):
            mentions[m.lower()] += 1

        # hashtags
        for h in HASHTAG_RE.findall(text):
            hashtags[h.lower()] += 1

        # words — use a simple regex tokenizer (avoids NLTK punkt/punkt_tab issues)
        tokens = re.findall(r"\b[a-zA-Z]+\b", text.lower())
        for tok in tokens:
            if tok in stopwords:
                continue
            words[tok] += 1

        # language
        if t.get("lang"):
            langs[t.get("lang")] += 1

        # engagement score from public_metrics if present
        pm = t.get("public_metrics") or {}
        engagement = 0
        try:
            engagement = int(pm.get("retweet_count", 0)) + int(pm.get("reply_count", 0)) + int(pm.get("like_count", 0)) + int(pm.get("quote_count", 0))
        except Exception:
            engagement = 0

        rep_candidates.append((engagement, len(text), t))

    # choose representative tweets: top by engagement, then length
    rep_sorted = sorted(rep_candidates, key=lambda x: (-x[0], -x[1]))[:top]

    # prepare summary text
    lines: List[str] = []
    lines.append(f"Tweet summary for {total} tweets")
    if dates:
        lines.append(f"Time span: {min(dates).isoformat()} to {max(dates).isoformat()}")
    lines.append("")

    def top_list(counter: Counter, n=10):
        return counter.most_common(n)

    lines.append("Top hashtags:")
    for k, v in top_list(hashtags, 20):
        lines.append(f"  #{k}: {v}")
    if not hashtags:
        lines.append("  (none)")
    lines.append("")

    lines.append("Top mentions:")
    for k, v in top_list(mentions, 20):
        lines.append(f"  @{k}: {v}")
    if not mentions:
        lines.append("  (none)")
    lines.append("")

    lines.append("Top words:")
    for k, v in top_list(words, 30):
        lines.append(f"  {k}: {v}")
    if not words:
        lines.append("  (none)")
    lines.append("")

    lines.append("Language distribution:")
    for k, v in top_list(langs, 20):
        lines.append(f"  {k}: {v}")
    if not langs:
        lines.append("  (unknown)")
    lines.append("")

    lines.append("Representative tweets:")
    for eng, length, t in rep_sorted:
        tid = str(t.get("_id") or t.get("id") or "")
        auth = t.get("author_id") or ""
        text = (t.get("text") or "").replace("\n", " ")
        lines.append(f"- id={tid} author={auth} engagement={eng} len={length}")
        # include short excerpt
        excerpt = text if len(text) <= 240 else text[:237] + "..."
        lines.append(f"  {excerpt}")
        lines.append("")

    summary = "\n".join(lines)

    # write file
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(summary)

    logger.info("Wrote summary to %s", out_path)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Summarize collected tweets and write a text file")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tweets to read (0 = all)")
    parser.add_argument("--top", type=int, default=5, help="Number of representative tweets to include")
    parser.add_argument("--out", type=str, default="artifacts/tweet_summary.txt", help="Output summary file")

    args = parser.parse_args()

    mongo_uri = get_env_var("MONGODB_CONNECT")
    summary = summarize_tweets(mongo_uri, limit=args.limit, top=args.top, out_path=args.out)
    print(summary)


if __name__ == "__main__":
    main()
