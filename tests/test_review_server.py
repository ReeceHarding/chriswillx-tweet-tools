from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tweet_extractor.review_server import ReviewConfig, ReviewStore


class ReviewStoreTest(unittest.TestCase):
    def test_tweets_are_sorted_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            tweets_path = data_dir / "tweets.jsonl"
            tweets = [
                {"id": "old", "created_at": "Mon Jan 01 00:00:00 +0000 2024", "text": "old"},
                {"id": "new", "created_at": "Tue Jan 02 00:00:00 +0000 2024", "text": "new"},
            ]
            tweets_path.write_text("".join(json.dumps(tweet) + "\n" for tweet in tweets), encoding="utf-8")
            store = ReviewStore(ReviewConfig(data_dir=data_dir, host="127.0.0.1", port=0))

            self.assertEqual([tweet["id"] for tweet in store.tweets()], ["new", "old"])

    def test_decisions_and_undo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ReviewStore(ReviewConfig(data_dir=Path(tmp), host="127.0.0.1", port=0))
            store.add_decision("1", "keep", "desktop")
            store.add_decision("2", "reject", "mobile")

            self.assertEqual(store.decisions()["1"]["decision"], "keep")
            self.assertEqual(store.undo("mobile")["id"], "2")
            self.assertNotIn("2", store.decisions())

    def test_latest_decision_wins_across_devices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ReviewStore(ReviewConfig(data_dir=Path(tmp), host="127.0.0.1", port=0))
            store.add_decision("1", "keep", "desktop")
            store.add_decision("1", "reject", "mobile")

            self.assertEqual(store.decisions()["1"]["decision"], "reject")
            self.assertEqual(store.undo("mobile")["id"], "1")
            self.assertEqual(store.decisions()["1"]["decision"], "keep")

    def test_migrates_jsonl_decisions_to_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / "review_decisions.jsonl").write_text(
                json.dumps({"id": "1", "decision": "keep", "created_at": "2026-01-01T00:00:00+00:00"})
                + "\n",
                encoding="utf-8",
            )

            store = ReviewStore(ReviewConfig(data_dir=data_dir, host="127.0.0.1", port=0))

            self.assertEqual(store.decisions()["1"]["decision"], "keep")
            self.assertTrue((data_dir / "review.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
