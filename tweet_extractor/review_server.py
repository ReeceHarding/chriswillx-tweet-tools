from __future__ import annotations

import argparse
import json
import mimetypes
import socket
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "review_static"


@dataclass(frozen=True)
class ReviewConfig:
    data_dir: Path
    host: str
    port: int
    backend: str = "sqlite"
    supabase_url: str = ""
    supabase_key: str = ""

    @property
    def tweets_path(self) -> Path:
        return self.data_dir / "tweets.jsonl"

    @property
    def decisions_path(self) -> Path:
        return self.data_dir / "review_decisions.jsonl"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "review.sqlite3"


class ReviewStore:
    def __init__(self, config: ReviewConfig) -> None:
        self.config = config
        self._lock = threading.Lock()
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._migrate_jsonl_decisions()

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
        latest: dict[str, dict[str, Any]] = {}
        with self._db() as db:
            rows = db.execute(
                "SELECT tweet_id, decision, created_at, device_id FROM decisions ORDER BY created_at ASC"
            ).fetchall()
        for tweet_id, decision, created_at, device_id in rows:
            latest[str(tweet_id)] = {
                "id": str(tweet_id),
                "decision": decision,
                "created_at": created_at,
                "device_id": device_id,
            }
        return latest

    def decision_events(self) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute(
                "SELECT tweet_id, decision, created_at, device_id FROM decision_events ORDER BY event_id ASC"
            ).fetchall()
        return [
            {"id": str(tweet_id), "decision": decision, "created_at": created_at, "device_id": device_id}
            for tweet_id, decision, created_at, device_id in rows
        ]

    def add_decision(self, tweet_id: str, decision: str, device_id: str = "") -> dict[str, Any]:
        if decision not in {"keep", "reject"}:
            raise ValueError("decision must be keep or reject")
        if not tweet_id:
            raise ValueError("tweet id is required")
        event = {
            "id": tweet_id,
            "decision": decision,
            "created_at": datetime.now(UTC).isoformat(),
            "device_id": device_id,
        }
        with self._lock:
            with self._db() as db:
                db.execute(
                    """
                    INSERT INTO decision_events (tweet_id, decision, created_at, device_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (tweet_id, decision, event["created_at"], device_id),
                )
                db.execute(
                    """
                    INSERT INTO decisions (tweet_id, decision, created_at, device_id)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(tweet_id) DO UPDATE SET
                      decision = excluded.decision,
                      created_at = excluded.created_at,
                      device_id = excluded.device_id
                    """,
                    (tweet_id, decision, event["created_at"], device_id),
                )
        return event

    def undo(self, device_id: str = "") -> dict[str, Any] | None:
        with self._lock:
            with self._db() as db:
                if device_id:
                    row = db.execute(
                        """
                        SELECT event_id, tweet_id, decision, created_at, device_id
                        FROM decision_events
                        WHERE device_id = ?
                        ORDER BY event_id DESC
                        LIMIT 1
                        """,
                        (device_id,),
                    ).fetchone()
                else:
                    row = db.execute(
                        """
                        SELECT event_id, tweet_id, decision, created_at, device_id
                        FROM decision_events
                        ORDER BY event_id DESC
                        LIMIT 1
                        """
                    ).fetchone()
                if not row:
                    return None

                event_id, tweet_id, decision, created_at, row_device_id = row
                db.execute("DELETE FROM decision_events WHERE event_id = ?", (event_id,))
                previous = db.execute(
                    """
                    SELECT decision, created_at, device_id
                    FROM decision_events
                    WHERE tweet_id = ?
                    ORDER BY event_id DESC
                    LIMIT 1
                    """,
                    (tweet_id,),
                ).fetchone()
                if previous:
                    db.execute(
                        "UPDATE decisions SET decision = ?, created_at = ?, device_id = ? WHERE tweet_id = ?",
                        (previous[0], previous[1], previous[2], tweet_id),
                    )
                else:
                    db.execute("DELETE FROM decisions WHERE tweet_id = ?", (tweet_id,))
                removed = {
                    "id": str(tweet_id),
                    "decision": decision,
                    "created_at": created_at,
                    "device_id": row_device_id,
                }
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

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.config.db_path)

    @contextmanager
    def _db(self):
        db = self._connect()
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def _init_db(self) -> None:
        with self._db() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS decisions (
                    tweet_id TEXT PRIMARY KEY,
                    decision TEXT NOT NULL CHECK(decision IN ('keep', 'reject')),
                    created_at TEXT NOT NULL,
                    device_id TEXT NOT NULL DEFAULT ''
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK(decision IN ('keep', 'reject')),
                    created_at TEXT NOT NULL,
                    device_id TEXT NOT NULL DEFAULT ''
                )
                """
            )
            db.execute("CREATE INDEX IF NOT EXISTS idx_decision_events_tweet ON decision_events(tweet_id)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_decision_events_device ON decision_events(device_id)")

    def _migrate_jsonl_decisions(self) -> None:
        if not self.config.decisions_path.exists():
            return
        with self._db() as db:
            existing = db.execute("SELECT COUNT(*) FROM decision_events").fetchone()[0]
        if existing:
            return
        with self.config.decisions_path.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                event = json.loads(line)
                tweet_id = str(event.get("id", ""))
                decision = str(event.get("decision", ""))
                if tweet_id and decision in {"keep", "reject"}:
                    created_at = str(event.get("created_at") or datetime.now(UTC).isoformat())
                    device_id = str(event.get("device_id") or "migrated")
                    with self._lock, self._connect() as db:
                        db.execute(
                            """
                            INSERT INTO decision_events (tweet_id, decision, created_at, device_id)
                            VALUES (?, ?, ?, ?)
                            """,
                            (tweet_id, decision, created_at, device_id),
                        )
                        db.execute(
                            """
                            INSERT INTO decisions (tweet_id, decision, created_at, device_id)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(tweet_id) DO UPDATE SET
                              decision = excluded.decision,
                              created_at = excluded.created_at,
                              device_id = excluded.device_id
                            """,
                            (tweet_id, decision, created_at, device_id),
                        )

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


class SupabaseReviewStore:
    def __init__(self, config: ReviewConfig) -> None:
        if not config.supabase_url or not config.supabase_key:
            raise RuntimeError("Supabase backend requires SUPABASE_URL and SUPABASE_KEY or secret files.")
        self.config = config
        self._lock = threading.Lock()
        self.supabase_url = config.supabase_url.rstrip("/")
        self.supabase_key = config.supabase_key

    def tweets(self) -> list[dict[str, Any]]:
        rows = self._select_all(
            "review_tweets",
            {
                "select": "id,created_at_text,sort_timestamp,username,text,url,retweets,replies,likes,quotes,bookmarks,views,raw",
                "order": "sort_timestamp.desc",
            },
        )
        return [self._tweet_view(row) for row in rows]

    def decisions(self) -> dict[str, dict[str, Any]]:
        rows = self._select_all(
            "review_decisions",
            {"select": "tweet_id,decision,created_at,device_id", "order": "created_at.asc"},
        )
        return {
            str(row["tweet_id"]): {
                "id": str(row["tweet_id"]),
                "decision": row["decision"],
                "created_at": row["created_at"],
                "device_id": row.get("device_id", ""),
            }
            for row in rows
        }

    def add_decision(self, tweet_id: str, decision: str, device_id: str = "") -> dict[str, Any]:
        if decision not in {"keep", "reject"}:
            raise ValueError("decision must be keep or reject")
        if not tweet_id:
            raise ValueError("tweet id is required")
        event = {
            "tweet_id": tweet_id,
            "decision": decision,
            "created_at": datetime.now(UTC).isoformat(),
            "device_id": device_id,
        }
        with self._lock:
            self._request("POST", "/rest/v1/review_decision_events", body=event)
            self._request(
                "POST",
                "/rest/v1/review_decisions",
                query={"on_conflict": "tweet_id"},
                body=event,
                prefer="resolution=merge-duplicates",
            )
        return {"id": tweet_id, "decision": decision, "created_at": event["created_at"], "device_id": device_id}

    def undo(self, device_id: str = "") -> dict[str, Any] | None:
        query = {
            "select": "event_id,tweet_id,decision,created_at,device_id",
            "order": "event_id.desc",
            "limit": "1",
        }
        if device_id:
            query["device_id"] = f"eq.{device_id}"
        rows = self._request("GET", "/rest/v1/review_decision_events", query=query)
        if not rows:
            return None

        row = rows[0]
        tweet_id = str(row["tweet_id"])
        with self._lock:
            self._request("DELETE", "/rest/v1/review_decision_events", query={"event_id": f"eq.{row['event_id']}"})
            previous = self._request(
                "GET",
                "/rest/v1/review_decision_events",
                query={
                    "select": "decision,created_at,device_id",
                    "tweet_id": f"eq.{tweet_id}",
                    "order": "event_id.desc",
                    "limit": "1",
                },
            )
            if previous:
                replacement = {
                    "tweet_id": tweet_id,
                    "decision": previous[0]["decision"],
                    "created_at": previous[0]["created_at"],
                    "device_id": previous[0].get("device_id", ""),
                }
                self._request(
                    "POST",
                    "/rest/v1/review_decisions",
                    query={"on_conflict": "tweet_id"},
                    body=replacement,
                    prefer="resolution=merge-duplicates",
                )
            else:
                self._request("DELETE", "/rest/v1/review_decisions", query={"tweet_id": f"eq.{tweet_id}"})

        return {
            "id": tweet_id,
            "decision": row["decision"],
            "created_at": row["created_at"],
            "device_id": row.get("device_id", ""),
        }

    def export_kept(self) -> Path:
        decisions = self.decisions()
        kept_ids = {tweet_id for tweet_id, event in decisions.items() if event.get("decision") == "keep"}
        output = self.config.data_dir / "kept_tweets.jsonl"
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as file:
            for tweet in self.tweets():
                if tweet["id"] in kept_ids:
                    file.write(json.dumps(tweet["raw"], ensure_ascii=False) + "\n")
        return output

    def sync_from_local_files(self) -> tuple[int, int]:
        local = ReviewStore(self.config)
        tweets = local.tweets()
        decisions = local.decisions()
        tweet_rows = [self._remote_tweet_row(tweet) for tweet in tweets if tweet.get("id")]
        tweet_count = self._upsert_many("review_tweets", tweet_rows, "id")
        decision_rows = [
            {
                "tweet_id": tweet_id,
                "decision": event["decision"],
                "created_at": event["created_at"],
                "device_id": event.get("device_id", ""),
            }
            for tweet_id, event in decisions.items()
        ]
        decision_count = self._upsert_many("review_decisions", decision_rows, "tweet_id")
        if decision_rows:
            self._request("POST", "/rest/v1/review_decision_events", body=decision_rows)
        return tweet_count, decision_count

    def _select_all(self, table: str, query: dict[str, str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        page_size = 1000
        while True:
            page = self._request(
                "GET",
                f"/rest/v1/{table}",
                query=query,
                extra_headers={"Range": f"{offset}-{offset + page_size - 1}"},
            )
            if not page:
                break
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def _upsert_many(self, table: str, rows: list[dict[str, Any]], conflict_key: str) -> int:
        count = 0
        chunk_size = 500
        for index in range(0, len(rows), chunk_size):
            chunk = rows[index : index + chunk_size]
            if not chunk:
                continue
            self._request(
                "POST",
                f"/rest/v1/{table}",
                query={"on_conflict": conflict_key},
                body=chunk,
                prefer="resolution=merge-duplicates",
            )
            count += len(chunk)
        return count

    def _request(
        self,
        method: str,
        path: str,
        query: dict[str, str] | None = None,
        body: Any | None = None,
        prefer: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self.supabase_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        headers = {
            "Accept": "application/json",
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
        }
        if prefer:
            headers["Prefer"] = prefer
        if extra_headers:
            headers.update(extra_headers)
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
            if "Prefer" not in headers:
                headers["Prefer"] = "return=representation"
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=60) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload) if payload else []
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase request failed ({error.code}): {detail}") from error

    @staticmethod
    def _remote_tweet_row(tweet: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": tweet["id"],
            "created_at_text": tweet["created_at"],
            "sort_timestamp": tweet["sort_timestamp"],
            "username": tweet["username"],
            "text": tweet["text"],
            "url": tweet["url"],
            "retweets": tweet["retweets"],
            "replies": tweet["replies"],
            "likes": tweet["likes"],
            "quotes": tweet["quotes"],
            "bookmarks": tweet["bookmarks"],
            "views": tweet["views"],
            "raw": tweet["raw"],
        }

    @staticmethod
    def _tweet_view(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(row.get("id", "")),
            "created_at": row.get("created_at_text", ""),
            "sort_timestamp": row.get("sort_timestamp", 0),
            "text": row.get("text", ""),
            "url": row.get("url", ""),
            "username": row.get("username", ""),
            "retweets": number_or_zero(row.get("retweets")),
            "replies": number_or_zero(row.get("replies")),
            "likes": number_or_zero(row.get("likes")),
            "quotes": number_or_zero(row.get("quotes")),
            "bookmarks": number_or_zero(row.get("bookmarks")),
            "views": number_or_zero(row.get("views")),
            "raw": row.get("raw", {}),
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
            if parsed.path == "/api/sync":
                self._send_json({"decisions": store.decisions(), "status": self._status()})
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
                event = store.add_decision(
                    str(payload.get("id", "")),
                    str(payload.get("decision", "")),
                    str(payload.get("device_id", "")),
                )
                self._send_json({"ok": True, "event": event, "status": self._status()})
                return
            if parsed.path == "/api/undo":
                payload = self._read_json()
                removed = store.undo(str(payload.get("device_id", "")))
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
                "db_path": str(store.config.db_path),
                "backend": store.config.backend,
                "remote_url": store.config.supabase_url if store.config.backend == "supabase" else "",
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
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--backend", choices=["sqlite", "supabase"], default="sqlite")
    parser.add_argument("--supabase-url", default="")
    parser.add_argument("--supabase-key", default="")
    parser.add_argument("--supabase-url-file", type=Path, default=Path(".secrets/supabase_url"))
    parser.add_argument("--supabase-key-file", type=Path, default=Path(".secrets/supabase_key"))
    parser.add_argument("--sync-remote-on-start", action="store_true")
    parser.add_argument("--sync-remote-only", action="store_true")
    return parser


def local_lan_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return ""


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ReviewConfig(
        data_dir=args.data_dir,
        host=args.host,
        port=args.port,
        backend=args.backend,
        supabase_url=load_secret_arg(args.supabase_url, args.supabase_url_file),
        supabase_key=load_secret_arg(args.supabase_key, args.supabase_key_file),
    )
    if config.backend == "supabase":
        store = SupabaseReviewStore(config)
        if args.sync_remote_on_start or args.sync_remote_only:
            tweet_count, decision_count = store.sync_from_local_files()
            print(f"Synced {tweet_count} tweets and {decision_count} decisions to Supabase.")
            if args.sync_remote_only:
                return 0
    else:
        store = ReviewStore(config)
    server = ThreadingHTTPServer((config.host, config.port), make_handler(store))
    display_host = "127.0.0.1" if config.host in {"0.0.0.0", ""} else config.host
    print(f"Review app running at http://{display_host}:{config.port}")
    if config.host in {"0.0.0.0", ""}:
        lan_ip = local_lan_ip()
        if lan_ip:
            print(f"LAN/mobile URL: http://{lan_ip}:{config.port}")
    print(f"Backend: {config.backend}")
    print(f"Reading tweets from {config.tweets_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def load_secret_arg(value: str, path: Path) -> str:
    if value:
        return value.strip()
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
