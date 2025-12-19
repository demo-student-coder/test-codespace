#!/usr/bin/env python3
"""Visualize a Twitter user mention network saved as GEXF or read from MongoDB.

Creates a PNG snapshot of the most connected portion of the network for readability.

Environment variables:
- MONGODB_CONNECT: MongoDB connection string (used if GEXF absent)

Usage:
- python scripts/visualize_network.py --gexf user_network.gexf --out user_network.png --top 100
- python scripts/visualize_network.py --out user_network.png --top 100 (will build GEXF from DB if missing)
"""

import os
import sys
import logging
from typing import Optional

import argparse
import networkx as nx
import matplotlib.pyplot as plt
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_graph_from_gexf(path: str) -> nx.DiGraph:
    logger.info("Loading graph from %s", path)
    return nx.read_gexf(path)


def build_graph_from_db(mongo_uri: str) -> nx.DiGraph:
    client = MongoClient(mongo_uri)
    db = client["demo"]
    coll = db["user_network"]
    G = nx.DiGraph()
    for doc in coll.find():
        src = str(doc.get("source"))
        tgt = str(doc.get("target"))
        w = int(doc.get("count", 1))
        G.add_edge(src, tgt, weight=w)
    logger.info("Built graph from DB with %d nodes and %d edges", G.number_of_nodes(), G.number_of_edges())
    return G


def make_plot(G: nx.DiGraph, out_path: str, top_n: int = 100):
    if G.number_of_nodes() == 0:
        logger.error("Graph is empty; nothing to plot")
        return

    # Use weighted degree to rank nodes
    deg = dict(G.degree(weight="weight"))
    top_nodes = sorted(deg.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    nodes_set = set(n for n, _ in top_nodes)
    # Include neighbors to give context
    neighbors = set()
    for n in list(nodes_set):
        neighbors.update(G.successors(n))
        neighbors.update(G.predecessors(n))
    sub_nodes = list(nodes_set | neighbors)
    H = G.subgraph(sub_nodes).copy()

    # Layout
    pos = nx.spring_layout(H, k=0.7, seed=42)

    # Node sizes by degree
    sizes = [max(50, int(500 * (deg.get(n, 0) / (deg[top_nodes[0][0]] or 1)))) for n in H.nodes()]

    plt.figure(figsize=(12, 9))
    nx.draw_networkx_nodes(H, pos, node_size=sizes, node_color="#1f78b4", alpha=0.9)
    nx.draw_networkx_edges(H, pos, arrowstyle="->", arrowsize=8, edge_color="#333333", alpha=0.6)

    # Label only top nodes for readability
    labels = {n: n for n, _ in top_nodes if n in H}
    nx.draw_networkx_labels(H, pos, labels=labels, font_size=8)

    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    logger.info("Saved visualization to %s", out_path)


def get_env_var(name: str) -> str:
    v = os.getenv(name)
    if not v:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return v


def main():
    parser = argparse.ArgumentParser(description="Visualize Twitter user mention network")
    parser.add_argument("--gexf", type=str, default="user_network.gexf", help="Path to input GEXF (optional)")
    parser.add_argument("--out", type=str, default="user_network.png", help="Output PNG path")
    parser.add_argument("--top", type=int, default=100, help="Number of top nodes to focus on")
    parser.add_argument("--build-from-db", action="store_true", help="Force build graph from DB even if GEXF exists")

    args = parser.parse_args()

    G = None
    if os.path.exists(args.gexf) and not args.build_from_db:
        G = load_graph_from_gexf(args.gexf)
    else:
        mongo_uri = get_env_var("MONGODB_CONNECT")
        # If demo.user_network empty, attempt to build from tweets using the other script
        # fall back to reading build_user_network directly if needed
        G = build_graph_from_db(mongo_uri)
        if G.number_of_nodes() == 0:
            logger.info("demo.user_network empty; attempting to build from tweet_collection")
            # Reuse the logic from the other script here by reading tweet_collection and creating edges
            coll = MongoClient(mongo_uri)["demo"]["tweet_collection"]
            projection = {"author_id": 1, "text": 1}
            tweets = list(coll.find({}, projection))
            # Create temporary graph
            from collections import defaultdict
            import re
            edge_counts = defaultdict(int)
            mention_re = re.compile(r"@([A-Za-z0-9_]{1,15})")
            for t in tweets:
                src = t.get("author_id")
                if not src:
                    continue
                text = t.get("text", "") or ""
                seen = set()
                for m in mention_re.findall(text):
                    tgt = "username:" + m.lower()
                    if tgt in seen:
                        continue
                    edge_counts[(str(src), tgt)] += 1
                    seen.add(tgt)
            G = nx.DiGraph()
            for (s, t), w in edge_counts.items():
                G.add_edge(s, t, weight=w)

    if G.number_of_nodes() == 0:
        logger.error("No graph data available to visualize")
        sys.exit(1)

    make_plot(G, args.out, top_n=args.top)


if __name__ == "__main__":
    main()
