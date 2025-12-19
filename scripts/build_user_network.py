#!/usr/bin/env python3
"""Build a directed Twitter user network from tweets in MongoDB.

Edges are from the tweet author -> each mentioned user.

Environment variables:
- MONGODB_CONNECT: MongoDB connection string

Usage examples:
- python scripts/build_user_network.py --store --out user_network.gexf --limit 100
- python scripts/build_user_network.py --dry-run --limit 100
"""

import os
import sys
import logging
from collections import defaultdict
from typing import Dict, Tuple
import re

import argparse
from pymongo import MongoClient
import networkx as nx

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_env_var(name: str) -> str:
    v = os.getenv(name)
    if not v:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return v


def load_tweets(mongo_uri: str, limit: int = 0):
    client = MongoClient(mongo_uri)
    db = client["demo"]
    coll = db["tweet_collection"]

    # include `text` so we can extract @mentions when `entities` is not present
    projection = {"author_id": 1, "entities.mentions": 1, "text": 1}
    cursor = coll.find({}, projection)
    if limit and limit > 0:
        cursor = cursor.limit(limit)
    return list(cursor)


def build_edge_counts(tweets) -> Dict[Tuple[str, str], int]:
    edge_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    mention_re = re.compile(r"@([A-Za-z0-9_]{1,15})")
    for t in tweets:
        source = t.get("author_id")
        if not source:
            continue
        entities = t.get("entities") or {}
        mentions = entities.get("mentions", []) if isinstance(entities, dict) else []

        seen_targets = set()
        # Use structured mentions if available
        for m in mentions:
            if not isinstance(m, dict):
                continue
            target_id = m.get("id")
            username = m.get("username") or m.get("screen_name")
            if target_id:
                target = str(target_id)
            elif username:
                target = "username:" + username.lower()
            else:
                continue
            if target in seen_targets:
                continue
            edge_counts[(str(source), target)] += 1
            seen_targets.add(target)

        # Fallback: parse the tweet text for @mentions
        text = t.get("text", "") or ""
        for match in mention_re.findall(text):
            target = "username:" + match.lower()
            if target in seen_targets:
                continue
            edge_counts[(str(source), target)] += 1
            seen_targets.add(target)

    return edge_counts


def build_graph(edge_counts: Dict[Tuple[str, str], int]) -> nx.DiGraph:
    G = nx.DiGraph()
    for (src, tgt), w in edge_counts.items():
        G.add_edge(src, tgt, weight=w)
    return G


def store_network(mongo_uri: str, edge_counts: Dict[Tuple[str, str], int], clear_existing: bool = False):
    client = MongoClient(mongo_uri)
    db = client["demo"]
    coll = db["user_network"]

    if clear_existing:
        logger.info("Clearing existing 'user_network' collection")
        coll.delete_many({})

    # Upsert each edge document with a composite _id to avoid duplicates
    for (src, tgt), w in edge_counts.items():
        _id = f"{src}__{tgt}"
        coll.update_one({"_id": _id}, {"$set": {"source": src, "target": tgt, "count": w}}, upsert=True)
    logger.info("Stored %d edges to demo.user_network", len(edge_counts))


def main():
    parser = argparse.ArgumentParser(description="Build a Twitter user mention network from stored tweets")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tweets to read (0 = all)")
    parser.add_argument("--store", action="store_true", help="Store the edge counts into MongoDB collection demo.user_network")
    parser.add_argument("--clear", action="store_true", help="Clear existing demo.user_network collection before storing")
    parser.add_argument("--out", type=str, default=None, help="Write graph to file (GEXF format) if provided")
    parser.add_argument("--dry-run", action="store_true", help="Don't store or write any files; just print a summary")

    args = parser.parse_args()

    mongo_uri = get_env_var("MONGODB_CONNECT")
    tweets = load_tweets(mongo_uri, limit=args.limit)
    logger.info("Loaded %d tweets", len(tweets))

    edge_counts = build_edge_counts(tweets)
    logger.info("Found %d edges (unique source->target pairs)", len(edge_counts))

    if args.dry_run:
        # Print top edges
        top = sorted(edge_counts.items(), key=lambda kv: kv[1], reverse=True)[:20]
        logger.info("Top edges (source -> target : count):")
        for (src, tgt), w in top:
            logger.info("%s -> %s : %d", src, tgt, w)
        return

    G = build_graph(edge_counts)
    logger.info("Graph has %d nodes and %d edges", G.number_of_nodes(), G.number_of_edges())

    if args.out:
        try:
            nx.write_gexf(G, args.out)
            logger.info("Wrote graph to %s", args.out)
        except Exception as e:
            logger.exception("Failed to write graph file: %s", e)

    if args.store:
        store_network(mongo_uri, edge_counts, clear_existing=args.clear)


if __name__ == "__main__":
    main()
