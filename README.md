# Chris Williamson Tweet Extractor

This repository contains a resumable extractor for Chris Williamson's public X/Twitter posts.

## Public Dataset

This repo intentionally does not publish the full scraped tweet text dump. X's current Developer Policy restricts redistribution of X Content to third parties and generally allows downloadable sharing of Post IDs/User IDs rather than full Post objects.

The shareable index is:

- `tweet_ids/chriswillx_tweet_ids.csv`: tweet IDs and canonical X URLs, newest first.

Use the extractor or X directly to hydrate/open the tweets from those IDs.

This is not legal advice. If you want to publish full text, get explicit permission from the account owner and make sure your use complies with X's current Developer Policy and Terms.

## Review App

Start the keyboard review app:

```bash
python3 -m tweet_extractor.review_server --data-dir data/chriswillx --host 0.0.0.0 --port 8787
```

Open `http://127.0.0.1:8787`.

- Right arrow: keep
- Left arrow: reject
- Swipe right: keep
- Swipe left: reject
- `U`: undo
- `Export kept`: writes `data/chriswillx/kept_tweets.jsonl`

The server stores decisions in `data/chriswillx/review.sqlite3`, so desktop and phone sessions stay aligned through the same API. When started with `--host 0.0.0.0`, the terminal prints a LAN/mobile URL such as `http://192.168.x.x:8787`; open that on a phone connected to the same network.

The reliable path for a complete historical pull is X API v2 full-archive search:

```bash
export X_BEARER_TOKEN="..."
python3 -m tweet_extractor.cli \
  --backend x-api \
  --username ChrisWillx \
  --mode full-archive \
  --out-dir data/chriswillx
```

Unofficial fallback using `twscrape`:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install ".[twscrape]"
.venv/bin/twscrape add_cookie account_name "auth_token=...; ct0=..."
.venv/bin/python -m tweet_extractor.cli \
  --backend twscrape \
  --username ChrisWillx \
  --out-dir data/chriswillx
```

Hosted fallback using Apify:

```bash
mkdir -p .secrets
printf '%s' "$APIFY_TOKEN" > .secrets/apify_token
python3 -m tweet_extractor.cli \
  --backend apify \
  --username ChrisWillx \
  --out-dir data/chriswillx
```

The Apify backend uses `fastcrawler/tweet-x-twitter-scraper-0-2-1k-pay-per-result-v2` by default. It runs search windows like `from:ChrisWillx since:2024-01-01 until:2025-01-01`, stores each window as it finishes, and automatically splits a window if it hits the actor's 5,000 item cap.

Outputs:

- `tweets.jsonl`: normalized tweet records, one JSON object per line.
- `tweets.csv`: spreadsheet-friendly normalized records.
- `raw.jsonl`: raw API tweet objects for future reprocessing.
- `seen.sqlite3`: deduplication database, safe across resumes.
- `state.json`: checkpoint with pagination cursor and request metadata.

## Access Notes

X's full public archive requires a bearer token with access to `GET /2/tweets/search/all`. If your token does not have full-archive access, the command will stop with a clear error. You can use:

- `--backend x-api --mode full-archive` for the complete historical account export, subject to X account tier.
- `--backend x-api --mode user-timeline` for the user timeline endpoint, which is useful as a lower-tier smoke test but may not return the complete history.
- `--backend twscrape` for an unofficial scraper using logged-in X cookies/accounts stored by `twscrape`.
- `--backend apify` for the pay-per-result Apify actor, using `APIFY_TOKEN` or `.secrets/apify_token`.

By default the extractor includes replies, reposts, quotes, and original posts. Use `--exclude-replies` and `--exclude-retweets` if you want only original/quote posts.

## Resume

The extractor writes every page as it goes and stores the next pagination token in `state.json`. If it is interrupted or rate-limited, rerun the same command and it resumes from the last checkpoint:

```bash
python3 -m tweet_extractor.cli --username ChrisWillx --mode full-archive --out-dir data/chriswillx
```

## Validation

Run tests:

```bash
python3 -m unittest discover -s tests
```
