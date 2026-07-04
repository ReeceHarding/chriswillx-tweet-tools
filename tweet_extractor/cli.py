from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .apify_client import (
    DEFAULT_ACTOR_ID,
    ApifyClient,
    ApifyError,
    iter_date_windows,
    load_apify_token,
    parse_date,
    split_window,
)
from .models import TweetRecord
from .storage import TweetStore
from .x_api import XApiClient, XApiError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract public X/Twitter posts from a target account.")
    parser.add_argument(
        "--backend",
        choices=["x-api", "twscrape", "apify"],
        default="x-api",
        help="Data source backend. x-api is official; twscrape uses cookies; apify uses a hosted actor.",
    )
    parser.add_argument("--username", default="ChrisWillx", help="X/Twitter username without @.")
    parser.add_argument(
        "--mode",
        choices=["full-archive", "user-timeline"],
        default="full-archive",
        help="full-archive is required for complete historical exports.",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("data/chriswillx"))
    parser.add_argument("--bearer-token", default=os.environ.get("X_BEARER_TOKEN"))
    parser.add_argument("--exclude-replies", action="store_true")
    parser.add_argument("--exclude-retweets", action="store_true")
    parser.add_argument("--start-time", help="UTC RFC3339 timestamp, for example 2006-03-21T00:00:00Z.")
    parser.add_argument("--end-time", help="UTC RFC3339 timestamp.")
    parser.add_argument("--limit-pages", type=int, help="Testing/debug limit. Omit for full extraction.")
    parser.add_argument("--limit-tweets", type=int, help="Testing/debug tweet cap for twscrape. Omit for full extraction.")
    parser.add_argument("--twscrape-db", type=Path, default=Path("accounts.db"))
    parser.add_argument("--apify-token", default=os.environ.get("APIFY_TOKEN"))
    parser.add_argument("--apify-token-file", type=Path, default=Path(".secrets/apify_token"))
    parser.add_argument("--apify-actor-id", default=DEFAULT_ACTOR_ID)
    parser.add_argument("--apify-max-items", type=int, default=5000)
    parser.add_argument("--apify-window-days", type=int, default=365)
    parser.add_argument("--apify-start-date", default="2006-03-21")
    parser.add_argument(
        "--apify-end-date",
        default=(datetime.now(UTC).date()).isoformat(),
        help="Exclusive end date in YYYY-MM-DD format.",
    )
    parser.add_argument("--apify-timeout-seconds", type=int, default=900)
    parser.add_argument("--apify-retries", type=int, default=2)
    parser.add_argument("--no-rate-limit-sleep", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.backend == "apify":
        return run_apify(args)
    if args.backend == "twscrape":
        return asyncio.run(run_twscrape(args))
    return run_x_api(args)


def run_x_api(args: argparse.Namespace) -> int:
    client = XApiClient(
        args.bearer_token or "",
        sleep_on_rate_limit=not args.no_rate_limit_sleep,
    )

    with TweetStore(args.out_dir) as store:
        state = store.load_state()
        username = args.username.lstrip("@")
        user = client.user_by_username(username)
        user_id = str(user["id"])
        mode_state = state.get(args.mode, {})
        pagination_token = mode_state.get("next_token")
        page_count = 0
        new_count = 0

        print(f"Resolved @{username} to user id {user_id}", file=sys.stderr)
        while True:
            page_count += 1
            if args.mode == "full-archive":
                page = client.full_archive_page(
                    username,
                    pagination_token=pagination_token,
                    exclude_replies=args.exclude_replies,
                    exclude_retweets=args.exclude_retweets,
                    start_time=args.start_time,
                    end_time=args.end_time,
                )
            else:
                page = client.user_timeline_page(
                    user_id,
                    pagination_token=pagination_token,
                    exclude_replies=args.exclude_replies,
                    exclude_retweets=args.exclude_retweets,
                )

            records = [TweetRecord.from_api(tweet, username) for tweet in page.data]
            written = store.write_records(records)
            new_count += written

            pagination_token = page.meta.get("next_token")
            state[args.mode] = {
                "username": username,
                "user_id": user_id,
                "next_token": pagination_token,
                "last_page_result_count": page.meta.get("result_count", len(page.data)),
                "last_page_new_records": written,
                "total_seen": store.count_seen(),
                "updated_at": datetime.now(UTC).isoformat(),
                "complete": pagination_token is None,
            }
            store.save_state(state)
            print(
                f"Page {page_count}: fetched {len(records)}, wrote {written}, total seen {store.count_seen()}",
                file=sys.stderr,
            )

            if not pagination_token:
                break
            if args.limit_pages and page_count >= args.limit_pages:
                print("Stopped because --limit-pages was reached.", file=sys.stderr)
                break

        print(f"Done. Wrote {new_count} new records to {args.out_dir}", file=sys.stderr)
    return 0


async def run_twscrape(args: argparse.Namespace) -> int:
    try:
        from twscrape import API
    except ImportError as error:
        raise XApiError(
            "twscrape is not installed. Run: python3 -m venv .venv && "
            '.venv/bin/python -m pip install ".[twscrape]"'
        ) from error

    username = args.username.lstrip("@")
    api = API(str(args.twscrape_db))
    with TweetStore(args.out_dir) as store:
        state = store.load_state()
        user = await api.user_by_login(username)
        if user is None:
            raise XApiError(
                "twscrape could not resolve the user. This usually means there are no active "
                "X accounts/cookies in the twscrape database. Add cookies with: "
                '.venv/bin/twscrape add_cookie account_name "auth_token=...; ct0=..."'
            )

        limit = args.limit_tweets or -1
        if args.exclude_replies:
            stream: Any = api.user_tweets(int(user.id), limit=limit)
        else:
            stream = api.user_tweets_and_replies(int(user.id), limit=limit)

        batch: list[TweetRecord] = []
        fetched = 0
        written_total = 0
        async for tweet in stream:
            if args.exclude_retweets and getattr(tweet, "retweetedTweet", None) is not None:
                continue
            fetched += 1
            batch.append(TweetRecord.from_twscrape(tweet))
            if len(batch) >= 100:
                written_total += store.write_records(batch)
                batch = []
                state["twscrape"] = _twscrape_state(username, user, fetched, written_total, complete=False)
                store.save_state(state)
                print(f"Fetched {fetched}, wrote {written_total}, total seen {store.count_seen()}", file=sys.stderr)

        if batch:
            written_total += store.write_records(batch)

        state["twscrape"] = _twscrape_state(username, user, fetched, written_total, complete=True)
        store.save_state(state)
        print(f"Done. Fetched {fetched}, wrote {written_total} new records to {args.out_dir}", file=sys.stderr)
    return 0


def run_apify(args: argparse.Namespace) -> int:
    username = args.username.lstrip("@")
    token = load_apify_token(args.apify_token, args.apify_token_file)
    client = ApifyClient(token)
    start_date = parse_date(args.apify_start_date)
    end_date = parse_date(args.apify_end_date)
    if start_date >= end_date:
        raise XApiError("--apify-start-date must be before --apify-end-date.")

    pending = iter_date_windows(start_date, end_date, args.apify_window_days)
    completed: list[dict[str, Any]] = []
    written_total = 0

    with TweetStore(args.out_dir) as store:
        state = store.load_state()
        apify_state = state.get("apify", {})
        failed_windows: list[dict[str, Any]] = list(apify_state.get("failed_windows", []))
        if (
            apify_state.get("username") == username
            and apify_state.get("actor_id") == args.apify_actor_id
            and apify_state.get("pending_windows")
        ):
            pending = [
                (parse_date(item["start"]), parse_date(item["end"]))
                for item in apify_state.get("pending_windows", [])
            ]
            completed = list(apify_state.get("completed_windows", []))
            print(f"Resuming Apify export with {len(pending)} pending windows.", file=sys.stderr)
        while pending:
            window_start, window_end = pending.pop(0)
            query = _apify_query(username, window_start.isoformat(), window_end.isoformat(), args)
            actor_input = {
                "searchTerms": [query],
                "sortBy": "Latest",
                "maxItems": args.apify_max_items,
            }
            print(f"Running Apify window {window_start} to {window_end}", file=sys.stderr)
            try:
                result = client.run_actor_and_get_items(
                    args.apify_actor_id,
                    actor_input,
                    timeout_seconds=args.apify_timeout_seconds,
                    retries=args.apify_retries,
                )
            except ApifyError as error:
                split = split_window(window_start, window_end)
                if split:
                    pending = [split[0], split[1], *pending]
                    print(
                        f"Apify window failed; splitting {window_start} to {window_end}. Reason: {error}",
                        file=sys.stderr,
                    )
                else:
                    failed_windows.append(
                        {
                            "start": window_start.isoformat(),
                            "end": window_end.isoformat(),
                            "error": str(error),
                        }
                    )
                    print(
                        f"Apify single-day window failed; recording gap {window_start} to {window_end}.",
                        file=sys.stderr,
                    )
                state["apify"] = _apify_state(
                    username,
                    args.apify_actor_id,
                    completed,
                    pending,
                    failed_windows,
                    store.count_seen(),
                    written_total,
                    complete=not pending,
                )
                store.save_state(state)
                continue
            records = [TweetRecord.from_apify(item, username) for item in result.items if _looks_like_tweet(item)]
            written = store.write_records(records)
            written_total += written
            capped = len(records) >= args.apify_max_items
            split = split_window(window_start, window_end) if capped else None
            if split:
                pending = [split[0], split[1], *pending]
                complete = False
            else:
                completed.append(
                    {
                        "start": window_start.isoformat(),
                        "end": window_end.isoformat(),
                        "run_id": result.run_id,
                        "dataset_id": result.dataset_id,
                        "fetched": len(result.items),
                        "written": written,
                    }
                )
                complete = not pending

            state["apify"] = {
                **_apify_state(
                    username,
                    args.apify_actor_id,
                    completed,
                    pending,
                    failed_windows,
                    store.count_seen(),
                    written_total,
                    complete=complete,
                )
            }
            store.save_state(state)
            print(
                f"Window fetched {len(result.items)}, wrote {written}, total seen {store.count_seen()}",
                file=sys.stderr,
            )

    print(f"Done. Wrote {written_total} new records to {args.out_dir}", file=sys.stderr)
    return 0


def _apify_state(
    username: str,
    actor_id: str,
    completed: list[dict[str, Any]],
    pending: list[tuple[Any, Any]],
    failed_windows: list[dict[str, Any]],
    total_seen: int,
    written_total: int,
    *,
    complete: bool,
) -> dict[str, Any]:
    return {
        "username": username,
        "actor_id": actor_id,
        "completed_windows": completed,
        "pending_windows": [{"start": start.isoformat(), "end": end.isoformat()} for start, end in pending],
        "failed_windows": failed_windows,
        "total_seen": total_seen,
        "written_this_run": written_total,
        "updated_at": datetime.now(UTC).isoformat(),
        "complete": complete,
    }


def _apify_query(username: str, start_date: str, end_date: str, args: argparse.Namespace) -> str:
    filters = [f"from:{username}", f"since:{start_date}", f"until:{end_date}"]
    if args.exclude_replies:
        filters.append("-filter:replies")
    if args.exclude_retweets:
        filters.append("-filter:retweets")
    return " ".join(filters)


def _looks_like_tweet(item: dict[str, Any]) -> bool:
    tweet_id = item.get("id") or item.get("id_str") or item.get("tweetId")
    if str(tweet_id or "") in {"", "0"}:
        return False
    if item.get("text") == "mock data":
        return False
    item_type = item.get("type")
    return item_type in {None, "tweet"} and any(key in item for key in ("text", "createdAt", "created_at"))


def _twscrape_state(username: str, user: Any, fetched: int, written: int, *, complete: bool) -> dict[str, Any]:
    return {
        "username": username,
        "user_id": str(getattr(user, "id", "")),
        "fetched_this_run": fetched,
        "written_this_run": written,
        "updated_at": datetime.now(UTC).isoformat(),
        "complete": complete,
    }


def run() -> None:
    try:
        raise SystemExit(main())
    except (XApiError, ApifyError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    run()
