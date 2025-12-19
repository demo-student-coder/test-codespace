#!/usr/bin/env python3
"""Perform sentiment analysis on tweets stored in MongoDB and save a bar chart.

- Uses NLTK VADER to compute sentiment per tweet (positive/neutral/negative).
- Stores per-tweet sentiment to `demo.tweet_sentiment` (upsert) and summary to `demo.sentiment_summary`.
- Writes a bar chart PNG to `artifacts/sentiment_bar.png` by default.

Environment variables:
- MONGODB_CONNECT: MongoDB connection string

Usage:
- python scripts/sentiment_analysis.py --limit 100 --out artifacts/sentiment_bar.png --store
"""

import os
import sys
import logging
from collections import Counter
from datetime import datetime
from typing import Dict

import argparse
import matplotlib.pyplot as plt
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_env_var(name: str) -> str:
    v = os.getenv(name)
    if not v:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return v


def ensure_vader():
    try:
        # If resource already present this is a no-op
        nltk.data.find("sentiment/vader_lexicon.zip")
    except Exception:
        logger.info("Downloading vader_lexicon for NLTK (one-time)")
        nltk.download("vader_lexicon")


def analyze_tweets(mongo_uri: str, limit: int = 0, store: bool = False, out_path: str = "artifacts/sentiment_bar.png"):
    client = MongoClient(mongo_uri)
    db = client["demo"]
    tweets = db["tweet_collection"].find({}, {"text": 1, "author_id": 1})
    if limit and limit > 0:
        tweets = tweets.limit(limit)

    ensure_vader()
    sia = SentimentIntensityAnalyzer()

    counts = Counter()
    per_tweet = []

    for t in tweets:
        tid = str(t.get("_id") or t.get("id"))
        text = (t.get("text") or "")
        if not text:
            continue
        scores = sia.polarity_scores(text)
        compound = scores.get("compound", 0.0)
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        counts[label] += 1

        per_tweet.append({"_id": tid, "tweet_id": tid, "text": text[:1000], "author_id": t.get("author_id"), "scores": scores, "label": label, "analyzed_at": datetime.utcnow()})

    logger.info("Sentiment counts: %s", dict(counts))

    # Store per-tweet sentiments
    if store and per_tweet:
        s_coll = db["tweet_sentiment"]
        for doc in per_tweet:
            s_coll.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
        # store summary
        db["sentiment_summary"].insert_one({"counts": dict(counts), "created_at": datetime.utcnow()})
        logger.info("Stored %d tweet sentiment docs", len(per_tweet))

    # Make bar chart
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    categories = ["positive", "neutral", "negative"]
    values = [counts.get(c, 0) for c in categories]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(categories, values, color=["#2ca02c", "#ff7f0e", "#d62728"])
    plt.title("Tweet Sentiment Distribution")
    plt.ylabel("Count")
    plt.xlabel("Sentiment")

    # Add labels
    for bar, val in zip(bars, values):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height + 0.5, str(val), ha="center", va="bottom")

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()

    logger.info("Saved sentiment bar chart to %s", out_path)
    return dict(counts)


def main():
    parser = argparse.ArgumentParser(description="Analyze tweet sentiment and save a bar chart")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tweets to analyze (0 = all)")
    parser.add_argument("--out", type=str, default="artifacts/sentiment_bar.png", help="Output PNG path")
    parser.add_argument("--store", action="store_true", help="Store per-tweet sentiments in MongoDB")

    args = parser.parse_args()

    mongo_uri = get_env_var("MONGODB_CONNECT")
    analyze_tweets(mongo_uri, limit=args.limit, store=args.store, out_path=args.out)


if __name__ == "__main__":
    main()
