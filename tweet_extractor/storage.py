from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import TweetRecord


CSV_FIELDS = [
    "id",
    "created_at",
    "text",
    "author_id",
    "username",
    "conversation_id",
    "in_reply_to_user_id",
    "referenced_tweets",
    "retweet_count",
    "reply_count",
    "like_count",
    "quote_count",
    "bookmark_count",
    "impression_count",
    "lang",
    "possibly_sensitive",
]


class TweetStore:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.out_dir / "tweets.jsonl"
        self.raw_path = self.out_dir / "raw.jsonl"
        self.csv_path = self.out_dir / "tweets.csv"
        self.state_path = self.out_dir / "state.json"
        self.db_path = self.out_dir / "seen.sqlite3"
        self._db = sqlite3.connect(self.db_path)
        self._db.execute("CREATE TABLE IF NOT EXISTS seen_tweets (id TEXT PRIMARY KEY)")
        self._db.commit()
        self._ensure_csv_header()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "TweetStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def save_state(self, state: dict[str, Any]) -> None:
        tmp_path = self.state_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.state_path)

    def has_seen(self, tweet_id: str) -> bool:
        row = self._db.execute("SELECT 1 FROM seen_tweets WHERE id = ?", (tweet_id,)).fetchone()
        return row is not None

    def mark_seen(self, tweet_id: str) -> None:
        self._db.execute("INSERT OR IGNORE INTO seen_tweets (id) VALUES (?)", (tweet_id,))

    def write_records(self, records: list[TweetRecord]) -> int:
        new_records = []
        batch_seen = set()
        for record in records:
            if not record.id or record.id in batch_seen or self.has_seen(record.id):
                continue
            batch_seen.add(record.id)
            new_records.append(record)
        if not new_records:
            return 0

        with self.jsonl_path.open("a", encoding="utf-8") as jsonl_file, self.raw_path.open(
            "a", encoding="utf-8"
        ) as raw_file, self.csv_path.open("a", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
            for record in new_records:
                jsonl_file.write(json.dumps(record.to_json(), ensure_ascii=False) + "\n")
                raw_file.write(json.dumps(record.raw, ensure_ascii=False) + "\n")
                writer.writerow(record.to_csv_row())
                self.mark_seen(record.id)

        self._db.commit()
        return len(new_records)

    def count_seen(self) -> int:
        row = self._db.execute("SELECT COUNT(*) FROM seen_tweets").fetchone()
        return int(row[0])

    def _ensure_csv_header(self) -> None:
        if self.csv_path.exists() and self.csv_path.stat().st_size > 0:
            return
        with self.csv_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
            writer.writeheader()
