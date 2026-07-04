from __future__ import annotations

import argparse
import json
import mimetypes
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "review_static"


@dataclass(frozen=True)
class ReviewConfig:
    data_dir: Path
    host: str
    port: int

    @property
    def tweets_path(self) -> Path:
        return self.data_dir / "tweets.jsonl"

    @property
    def decisions_path(self) -> Path:
        return self.data_dir / "review_decisions.jsonl"


class ReviewStore:
    def __init__(self, config: ReviewConfig) -> None:
        self.config = config
        self._lock = threading.Lock()

    def tweets(self) -> list[dict[str, Any]]:
        if not self.config.tweets_path.exists():
            return []
        records = []
        with self.config.tweets_path.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                item = json.loads(line)
                records.append(self._tweet_view(item))
        records.sort(key=lambda item: item["sort_timestamp"], reverse=True)
        return records

    def decisions(self) -> dict[str, dict[str, Any]]:
        events = self.decision_events()
        latest: dict[str, dict[str, Any]] = {}
        for event in events:
            tweet_id = str(event.get("id", ""))
            if tweet_id:
                latest[tweet_id] = event
        return latest

    def decision_events(self) -> list[dict[str, Any]]:
        if not self.config.decisions_path.exists():
            return []
        events = []
        with self.config.decisions_path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    events.append(json.loads(line))
        return events

    def add_decision(self, tweet_id: str, decision: str) -> dict[str, Any]:
        if decision not in {"keep", "reject"}:
            raise ValueError("decision must be keep or reject")
        event = {
            "id": tweet_id,
            "decision": decision,
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self.config.decisions_path.open("a", encoding="utf-8") as file:
                file.write(json.dumps(event, ensure_ascii=False) + "\n")
        return event

    def undo(self) -> dict[str, Any] | None:
        with self._lock:
            events = self.decision_events()
            if not events:
                return None
            removed = events.pop()
            self.config.decisions_path.write_text(
                "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events),
                encoding="utf-8",
            )
            return removed

    def export_kept(self) -> Path:
        decisions = self.decisions()
        kept_ids = {tweet_id for tweet_id, event in decisions.items() if event.get("decision") == "keep"}
        output = self.config.data_dir / "kept_tweets.jsonl"
        with output.open("w", encoding="utf-8") as file:
            for tweet in self.tweets():
                if tweet["id"] in kept_ids:
                    file.write(json.dumps(tweet["raw"], ensure_ascii=False) + "\n")
        return output

    @staticmethod
    def _tweet_view(item: dict[str, Any]) -> dict[str, Any]:
        raw = item.get("raw", {})
        created_at = str(item.get("created_at") or raw.get("createdAt") or "")
        timestamp = parse_timestamp(created_at)
        metrics = item.get("public_metrics") or {}
        return {
            "id": str(item.get("id", "")),
            "created_at": created_at,
            "sort_timestamp": timestamp,
            "text": item.get("text", ""),
            "url": raw.get("url") or raw.get("twitterUrl") or f"https://x.com/{item.get('username')}/status/{item.get('id')}",
            "username": item.get("username", ""),
            "retweets": number_or_zero(metrics.get("retweet_count")),
            "replies": number_or_zero(metrics.get("reply_count")),
            "likes": number_or_zero(metrics.get("like_count")),
            "quotes": number_or_zero(metrics.get("quote_count")),
            "bookmarks": number_or_zero(metrics.get("bookmark_count")),
            "views": number_or_zero(metrics.get("impression_count")),
            "raw": item,
        }


def parse_timestamp(value: str) -> float:
    if not value:
        return 0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return 0


def number_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def make_handler(store: ReviewStore) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/tweets":
                self._handle_tweets(parsed.query)
                return
            if parsed.path == "/api/status":
                self._send_json(self._status())
                return
            if parsed.path in {"/", "/index.html"}:
                self._send_file(STATIC_DIR / "index.html")
                return
            if parsed.path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return
            self._send_file(STATIC_DIR / parsed.path.lstrip("/"))

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/decision":
                payload = self._read_json()
                event = store.add_decision(str(payload.get("id", "")), str(payload.get("decision", "")))
                self._send_json({"ok": True, "event": event, "status": self._status()})
                return
            if parsed.path == "/api/undo":
                removed = store.undo()
                self._send_json({"ok": True, "removed": removed, "status": self._status()})
                return
            if parsed.path == "/api/export-kept":
                output = store.export_kept()
                self._send_json({"ok": True, "path": str(output), "status": self._status()})
                return
            self.send_error(404)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_tweets(self, query: str) -> None:
            params = parse_qs(query)
            include_decided = params.get("include_decided", ["false"])[0] == "true"
            decisions = store.decisions()
            tweets = store.tweets()
            if not include_decided:
                tweets = [tweet for tweet in tweets if tweet["id"] not in decisions]
            self._send_json({"tweets": tweets, "decisions": decisions, "status": self._status()})

        def _status(self) -> dict[str, Any]:
            tweets = store.tweets()
            decisions = store.decisions()
            kept = sum(1 for event in decisions.values() if event.get("decision") == "keep")
            rejected = sum(1 for event in decisions.values() if event.get("decision") == "reject")
            return {
                "total": len(tweets),
                "decided": len(decisions),
                "remaining": max(0, len(tweets) - len(decisions)),
                "kept": kept,
                "rejected": rejected,
                "tweets_path": str(store.config.tweets_path),
                "decisions_path": str(store.config.decisions_path),
            }

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _send_json(self, payload: dict[str, Any]) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_file(self, path: Path) -> None:
            if not path.exists() or not path.is_file():
                self.send_error(404)
                return
            data = path.read_bytes()
            content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keyboard-first tweet review tool.")
    parser.add_argument("--data-dir", type=Path, default=Path("data/chriswillx"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ReviewConfig(data_dir=args.data_dir, host=args.host, port=args.port)
    store = ReviewStore(config)
    server = ThreadingHTTPServer((config.host, config.port), make_handler(store))
    print(f"Review app running at http://{config.host}:{config.port}")
    print(f"Reading tweets from {config.tweets_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
