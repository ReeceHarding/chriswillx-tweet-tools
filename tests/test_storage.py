from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tweet_extractor.models import TweetRecord
from tweet_extractor.storage import TweetStore


class TweetStoreTest(unittest.TestCase):
    def test_deduplicates_records_across_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TweetStore(Path(tmp))
            record = TweetRecord.from_api(
                {
                    "id": "1",
                    "created_at": "2026-01-01T00:00:00Z",
                    "text": "hello",
                    "author_id": "123",
                    "public_metrics": {"like_count": 2},
                },
                "ChrisWillx",
            )

            self.assertEqual(store.write_records([record]), 1)
            self.assertEqual(store.write_records([record]), 0)
            self.assertEqual(store.write_records([record, record]), 0)
            self.assertEqual(store.count_seen(), 1)

            lines = (Path(tmp) / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            store.close()

    def test_deduplicates_records_within_single_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TweetStore(Path(tmp))
            record = TweetRecord.from_api(
                {
                    "id": "1",
                    "created_at": "2026-01-01T00:00:00Z",
                    "text": "hello",
                    "author_id": "123",
                },
                "ChrisWillx",
            )

            self.assertEqual(store.write_records([record, record]), 1)
            self.assertEqual(store.count_seen(), 1)
            lines = (Path(tmp) / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            store.close()

    def test_apify_records_are_normalized(self) -> None:
        record = TweetRecord.from_apify(
            {
                "id": "123",
                "createdAt": "Sat Jul 04 12:00:00 +0000 2026",
                "text": "hello from apify",
                "author": {"id": "999", "userName": "ChrisWillx"},
                "likeCount": 7,
                "retweetCount": 2,
            },
            "ChrisWillx",
        )

        self.assertEqual(record.id, "123")
        self.assertEqual(record.username, "ChrisWillx")
        self.assertEqual(record.public_metrics["like_count"], 7)


if __name__ == "__main__":
    unittest.main()
