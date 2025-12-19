#!/usr/bin/env python3
"""Collect tweets matching "generative ai" and store them in MongoDB demo.tweet_collection.

Environment variables:
- MONGODB_CONNECT: MongoDB connection string (e.g., mongodb+srv://user:pass@cluster.example.com)
- TWITTER_API_KEY: Twitter API key or Bearer token (if using API key+secret, set TWITTER_API_SECRET to auto-exchange)
- TWITTER_API_SECRET: (optional) Twitter API secret (used with TWITTER_API_KEY to obtain a bearer token)
- TWITTER_BEARER_TOKEN: (optional) directly provide a bearer token

Usage: python scripts/collect_tweets.py
"""

import os
import sys
import time
import logging
from typing import List, Dict, Any, Optional

import requests
import base64
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# Configuration
QUERY = '"generative ai" -is:retweet'  # search for the phrase and exclude retweets
MAX_TO_COLLECT = 100
TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
TWEET_FIELDS = "created_at,author_id,lang,public_metrics"

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_env_var(name: str) -> str:
    v = os.getenv(name)
    if not v:
        logger.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return v


class TwitterClient:
    def __init__(self, bearer_token: Optional[str] = None, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initialize with either a bearer token, or an API key + secret pair (will be exchanged for a bearer token)."""
        if not bearer_token:
            if api_key and api_secret:
                bearer_token = self._get_bearer_from_key_secret(api_key, api_secret)
            elif api_key:
                # Some users pass the bearer token in TWITTER_API_KEY; accept that.
                bearer_token = api_key

        if not bearer_token:
            raise ValueError("Provide TWITTER_BEARER_TOKEN or both TWITTER_API_KEY and TWITTER_API_SECRET (or set TWITTER_API_KEY to a bearer token).")

        self.bearer_token = bearer_token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.bearer_token}"})

    def _get_bearer_from_key_secret(self, api_key: str, api_secret: str) -> str:
        """Exchange API key/secret for a bearer token using OAuth2 client credentials flow."""
        creds = f"{api_key}:{api_secret}"
        b64 = base64.b64encode(creds.encode()).decode()
        headers = {"Authorization": f"Basic {b64}", "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
        try:
            resp = requests.post("https://api.twitter.com/oauth2/token", data={"grant_type": "client_credentials"}, headers=headers, timeout=20)
            resp.raise_for_status()
            j = resp.json()
            token = j.get("access_token") or j.get("token") or j.get("bearer_token")
            if not token:
                raise ValueError("No access_token in token response")
            return token
        except Exception as e:
            logger.exception("Failed to obtain bearer token via API key/secret: %s", e)
            raise

    def search_recent(self, query: str, max_results: int = 100, next_token: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "query": query,
            "max_results": max(10, min(100, max_results)),
            "tweet.fields": TWEET_FIELDS,
        }
        if next_token:
            params["next_token"] = next_token

        resp = self.session.get(TWITTER_SEARCH_URL, params=params, timeout=20)
        if resp.status_code == 429:
            # rate limited
            logger.warning("Rate limited by Twitter API; sleeping for 60s")
            time.sleep(60)
            return self.search_recent(query, max_results=max_results, next_token=next_token)
        resp.raise_for_status()
        return resp.json()


def collect_tweets(twitter: TwitterClient, query: str, limit: int) -> List[Dict[str, Any]]:
    tweets: List[Dict[str, Any]] = []
    next_token = None
    while len(tweets) < limit:
        remaining = limit - len(tweets)
        want = min(100, remaining)
        try:
            data = twitter.search_recent(query=query, max_results=want, next_token=next_token)
        except Exception as e:
            logger.exception("Error fetching tweets: %s", e)
            break

        batch = data.get("data", [])
        if not batch:
            logger.info("No more tweets returned from API; stopping")
            break

        tweets.extend(batch)
        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        logger.info("Fetched %d tweets (total %d)", len(batch), len(tweets))
        if not next_token:
            break

    return tweets[:limit]


def store_tweets(mongo_uri: str, tweets: List[Dict[str, Any]]):
    client = MongoClient(mongo_uri)
    db = client["demo"]
    coll = db["tweet_collection"]

    # Use tweet ID as the Mongo _id to avoid duplicates
    docs = []
    for t in tweets:
        doc = t.copy()
        # ensure _id is a string and exists
        tid = doc.get("id")
        if tid is None:
            continue
        doc["_id"] = str(tid)
        docs.append(doc)

    if not docs:
        logger.info("No documents to insert")
        return

    try:
        result = coll.insert_many(docs, ordered=False)
        logger.info("Inserted %d new documents", len(result.inserted_ids))
    except BulkWriteError as bwe:
        # Some documents may already exist; count how many succeeded
        write_results = bwe.details
        inserted = write_results.get("nInserted", 0)
        logger.info("Inserted %d new documents (some duplicates were skipped)", inserted)
    except Exception as e:
        logger.exception("Unexpected error inserting documents: %s", e)


def main():
    mongo_uri = get_env_var("MONGODB_CONNECT")
    # Twitter credentials: prefer bearer token, but accept API key + secret pair and auto-exchange.
    bearer = os.getenv("TWITTER_BEARER_TOKEN")
    api_key = os.getenv("TWITTER_API_KEY")
    api_secret = os.getenv("TWITTER_API_SECRET")

    if bearer:
        twitter = TwitterClient(bearer_token=bearer)
    elif api_key and api_secret:
        twitter = TwitterClient(api_key=api_key, api_secret=api_secret)
    elif api_key:
        # Some users set TWITTER_API_KEY to the bearer token directly
        twitter = TwitterClient(bearer_token=api_key)
    else:
        logger.error("Missing Twitter credentials. Set TWITTER_BEARER_TOKEN or TWITTER_API_KEY and TWITTER_API_SECRET")
        sys.exit(1)

    logger.info("Collecting up to %d tweets for query: %s", MAX_TO_COLLECT, QUERY)
    tweets = collect_tweets(twitter, QUERY, MAX_TO_COLLECT)
    logger.info("Total tweets collected: %d", len(tweets))

    if tweets:
        logger.info("Storing tweets into MongoDB 'demo.tweet_collection'")
        store_tweets(mongo_uri, tweets)
        logger.info("Done")
    else:
        logger.info("No tweets collected; nothing to store")


if __name__ == "__main__":
    main()
