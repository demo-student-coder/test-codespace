# test-codespace

This repository contains tools to collect and analyze tweets about "generative ai" and to build a mention network and sentiment summary.

## What I did
- Collected up to 100 tweets matching the query "generative ai" and stored them in MongoDB `demo.tweet_collection`.
- Built a user mention network (edges from tweet author -> mentioned users) and saved it as GEXF and PNG (`artifacts/user_network.png`).
- Performed sentiment analysis (NLTK VADER) and stored per-tweet sentiment in `demo.tweet_sentiment` and a bar chart at `artifacts/sentiment_bar.png`.
- Produced a textual summary of the collected tweets at `artifacts/tweet_summary.txt`.

## Files added
- `scripts/collect_tweets.py` — collect tweets and store to MongoDB
- `scripts/build_user_network.py` — build mention network from stored tweets
- `scripts/visualize_network.py` — create a PNG snapshot of the network
- `scripts/sentiment_analysis.py` — analyze tweet sentiment and save a bar chart
- `scripts/summarize_tweets.py` — summarize hashtags, mentions, top words and representative tweets
- `index.html` — a simple report page (open in your browser) that embeds images and summary

## How to reproduce
1. Set environment variables:
   - `MONGODB_CONNECT` (required)
   - `TWITTER_BEARER_TOKEN` or `TWITTER_API_KEY` (and `TWITTER_API_SECRET` if you want to exchange)
2. Install dependencies: `pip install -r requirements.txt`
3. Collect tweets: `python scripts/collect_tweets.py`
4. Build network: `python scripts/build_user_network.py --out user_network.gexf --store`
5. Visualize network: `python scripts/visualize_network.py --gexf user_network.gexf --out artifacts/user_network.png`
6. Sentiment analysis: `python scripts/sentiment_analysis.py --limit 100 --out artifacts/sentiment_bar.png --store`
7. Summarize tweets: `python scripts/summarize_tweets.py --limit 100 --out artifacts/tweet_summary.txt`

## View the report
Open `index.html` in a browser or in Codespaces to see the charts and summary.

---

For details, see the `scripts/` directory.
